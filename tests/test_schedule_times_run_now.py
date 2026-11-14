"""'At set times' scheduling and Run Now in the background."""
import time
from datetime import datetime, timezone

import pytest

from conftest import Env, load_plugin, wait_for

ps = load_plugin()
CHI = ps.ZoneInfo("America/Chicago")


def at(y, mo, d, h, mi, tz=CHI):
    return datetime(y, mo, d, h, mi, tzinfo=tz).timestamp()


@pytest.mark.parametrize("raw, times, bad", [
    ("03:00, 15:00", [(3, 0), (15, 0)], []),
    ("15,3:30 ; 15:00", [(3, 30), (15, 0)], []),
    ("3pm, 25:00, 7:61, 9:05", [(9, 5)], ["3pm", "25:00", "7:61"]),
    ("", [], []),
])
def test_run_times_parsing(raw, times, bad):
    assert ps._run_times({"run_at_times": raw}) == (times, bad)


def test_schedule_kinds():
    assert ps._schedule({"scheduler_interval": "60"}) == ("every", 60.0)
    assert ps._schedule({"scheduler_interval": "times", "run_at_times": "03:00"}) == ("at", [(3, 0)])
    assert ps._schedule({"scheduler_interval": "times", "run_at_times": "nope"}) is None
    assert ps._schedule({"scheduler_interval": "off"}) is None


def test_next_and_previous_slot_in_local_time():
    times = [(3, 0), (15, 0)]
    now = at(2026, 11, 14, 12, 41)
    assert ps._next_slot(times, CHI, now) == at(2026, 11, 14, 15, 0)
    assert ps._prev_slot(times, CHI, now) == at(2026, 11, 14, 3, 0)
    assert ps._next_slot(times, CHI, at(2026, 11, 14, 15, 0)) == at(2026, 11, 15, 3, 0)  # strictly later


def test_slots_follow_daylight_saving():
    # US DST ended 2026-11-01 at 02:00: 03:00 local is 09:00 UTC after, 08:00 UTC before.
    before = ps._next_slot([(3, 0)], CHI, at(2026, 10, 30, 12, 0))
    after = ps._next_slot([(3, 0)], CHI, at(2026, 11, 1, 12, 0))
    assert datetime.fromtimestamp(before, timezone.utc).hour == 8
    assert datetime.fromtimestamp(after, timezone.utc).hour == 9


class Clock:
    def __init__(self, ps, start):
        self.now = start
        ps.time.time = lambda: self.now  # the plugin module's time.time


@pytest.fixture
def timed(tmp_path, monkeypatch):
    e = Env(tmp_path)
    e.settings.update({"scheduler_interval": "times", "run_at_times": "03:00, 15:00",
                       "display_timezone": "America/Chicago"})
    import types
    fake_time = types.SimpleNamespace(time=lambda: 0, sleep=lambda s: None)
    monkeypatch.setattr(e.ps, "time", fake_time)
    clock = Clock(e.ps, at(2026, 11, 14, 12, 0))
    outcomes = []
    monkeypatch.setattr(e.ps, "_do_one_run",
                        lambda settings, logger, manual=False, leader_id=None: outcomes.append(clock.now) or
                        {"status": e.next_status.pop(0) if e.next_status else "ok"})
    e.next_status = []
    ctrl = e.ps._Controller()
    e.clock, e.outcomes, e.ctrl = clock, outcomes, ctrl
    e.tick = lambda holding=True: e.ps._scheduler_tick(ctrl, e.redis, e.settings, e.logger, holding)
    return e


def test_runs_at_each_set_time_and_not_between(timed):
    e = timed
    e.tick(holding=False)
    assert e.outcomes == [] and e.ctrl.state["next_run_at"] == at(2026, 11, 14, 15, 0)
    e.clock.now = at(2026, 11, 14, 14, 59)
    e.tick()
    assert e.outcomes == []
    e.clock.now = at(2026, 11, 14, 15, 0) + 2  # a tick that lands just after the time still runs
    e.tick()
    assert e.outcomes == [e.clock.now]
    assert e.ctrl.state["next_run_at"] == at(2026, 11, 15, 3, 0)
    e.clock.now += 5
    e.tick()
    assert len(e.outcomes) == 1


def test_skip_retries_every_30_minutes_but_not_past_the_next_time(timed):
    e = timed
    e.settings["run_at_times"] = "03:00, 03:45"
    e.clock.now = at(2026, 11, 14, 2, 0)
    e.tick(holding=False)
    e.next_status = ["skipped", "skipped"]
    e.clock.now = at(2026, 11, 14, 3, 0)
    e.tick()
    assert e.ctrl.state["next_run_at"] == at(2026, 11, 14, 3, 30)
    e.clock.now = at(2026, 11, 14, 3, 30)
    e.tick()
    assert e.ctrl.state["next_run_at"] == at(2026, 11, 14, 3, 45)  # capped at the next set time
    e.clock.now = at(2026, 11, 14, 3, 45)
    e.tick()
    assert len(e.outcomes) == 3 and "pending_at" not in e.ctrl.state


def test_catch_up_after_a_restart_shortly_after_a_set_time(timed):
    e = timed
    e.clock.now = at(2026, 11, 14, 15, 10)
    e.tick(holding=False)
    assert e.outcomes == [e.clock.now]  # 15:00 was missed and nothing ran since: run once now
    assert e.ctrl.state["next_run_at"] == at(2026, 11, 15, 3, 0)


def test_no_catch_up_if_it_already_ran_or_long_ago(timed):
    e = timed
    e.ps._local_last_run.update({"at": at(2026, 11, 14, 15, 1), "status": "ok"})
    e.clock.now = at(2026, 11, 14, 15, 10)
    e.tick(holding=False)
    assert e.outcomes == []
    e.ps._local_last_run.clear()
    e.ctrl = e.ps._Controller()
    e.clock.now = at(2026, 11, 14, 17, 0)  # two hours late: wait for the next time instead
    e.ps._scheduler_tick(e.ctrl, None, e.settings, e.logger, False)
    assert e.outcomes == []


def test_changing_the_times_moves_the_next_run(timed):
    e = timed
    e.tick(holding=False)
    e.settings["run_at_times"] = "13:00"
    e.tick()
    assert e.ctrl.state["next_run_at"] == at(2026, 11, 14, 13, 0)


def test_status_for_set_times(env):
    e = env()
    e.settings.update({"scheduler_interval": "times", "run_at_times": "03:00, 3pm",
                       "display_timezone": "America/Chicago"})
    e.ps._local_last_run.clear()
    p = e.ps.Plugin()
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY))
    status = e.status(p)
    assert status.startswith("At 03:00 \u00b7 next ")
    assert "| Run at: couldn't read '3pm' (use 15:00) |" in status
    e.settings["run_at_times"] = ""
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY) is None, 3)  # the scheduler notices and stops
    assert e.status(p).startswith("Scheduler off ('Run at' has no valid times)")


def test_run_now_in_background_answers_at_once(env):
    e = env(run_seconds=0.6)
    p = e.ps.Plugin()
    p.stop(e.ctx)  # keep the scheduler out of this test
    started = time.time()
    out = p.run("run_now", {}, e.ctx)
    assert time.time() - started < 0.3
    assert out == {"status": "ok", "message": "Speedtest started. The result pops up here in about 30 seconds, and goes to Status and the log."}
    assert "Manual test in progress (started " in e.status(p)
    busy = p.run("run_now", {}, e.ctx)
    assert busy["status"] == "busy"  # checked before answering, never a false "started"
    assert wait_for(lambda: e.redis.get(e.ps._LAST_RUN_KEY), 3)
    status = e.status(p)
    assert "in progress" not in status and "(manual) \u2705 \u2b07 912.4" in status
    assert len(e.runs) == 1


def test_run_now_mentions_discord_when_set(env):
    e = env()
    e.settings["discord_webhook_url"] = "https://discord.example/x"
    e.ps._post_to_discord = lambda url, payload, logger: True
    out = e.ps._start_background_run(e.settings, e.logger)
    assert out["message"].endswith("goes to Discord, the log and Status.")
    assert wait_for(lambda: e.redis.get(e.ps._LAST_RUN_KEY), 3)


def test_run_now_wait_setting_keeps_the_old_popup(env):
    e = env()
    e.settings["run_now_wait"] = True
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    assert out["message"].startswith("⬇ 912.4 Mbps")


class FakeNotifications:
    """Stand-ins for Dispatcharr's core.models / core.utils notification pieces."""

    def __init__(self, monkeypatch):
        import sys
        import types
        self.rows, self.sent, self.dismissals_cleared = {}, [], []
        fake = self

        class Choices:
            WARNING, INFO, HIGH = "warning", "info", "high"

        class Manager:
            def update_or_create(self, notification_key, defaults):
                row = fake.rows.setdefault(notification_key, types.SimpleNamespace(notification_key=notification_key))
                row.__dict__.update(defaults)
                return row, False

        class SystemNotification:
            NotificationType = Choices
            Priority = Choices
            objects = Manager()

        class DismissalManager:
            def filter(self, notification):
                return types.SimpleNamespace(delete=lambda: fake.dismissals_cleared.append(notification.notification_key))

        class NotificationDismissal:
            objects = DismissalManager()

        models = types.ModuleType("core.models")
        models.SystemNotification, models.NotificationDismissal = SystemNotification, NotificationDismissal
        utils = types.ModuleType("core.utils")
        utils.send_websocket_notification = lambda n: fake.sent.append(dict(n.__dict__))
        dj = types.ModuleType("django.utils")
        dj.timezone = types.SimpleNamespace(now=lambda: datetime(2026, 11, 14, 20, 0, tzinfo=timezone.utc))
        for name, mod in {"core": types.ModuleType("core"), "core.models": models, "core.utils": utils,
                          "django": types.ModuleType("django"), "django.utils": dj}.items():
            monkeypatch.setitem(sys.modules, name, mod)


def test_background_result_pops_up_in_dispatcharr(env, monkeypatch):
    fake = FakeNotifications(monkeypatch)
    e = env()
    e.ps._start_background_run(e.settings, e.logger)
    assert wait_for(lambda: fake.sent, 3)
    n = fake.sent[0]
    assert n["notification_key"] == "packet-slapper-run-now-result"
    assert n["priority"] == "high" and n["notification_type"] == "info"
    assert n["title"] == "Packet Slapper: speedtest result"
    assert n["message"].startswith("\u2b07 912.4 Mbps")
    assert fake.dismissals_cleared == ["packet-slapper-run-now-result"]  # shows again even if dismissed before


def test_alerts_and_failures_pop_up_as_warnings(env, monkeypatch):
    fake = FakeNotifications(monkeypatch)
    e = env()
    e.settings["alert_min_download_mbps"] = 1000
    e.ps._start_background_run(e.settings, e.logger)
    assert wait_for(lambda: fake.sent, 3)
    assert fake.sent[-1]["notification_type"] == "warning"
    assert e.ps._notify_gui(e.logger, {"status": "error", "message": "\u274c Speedtest failed: timeout"})
    assert fake.sent[-1]["title"] == "Packet Slapper: speedtest failed"
    assert len(fake.rows) == 1  # one bell entry, replaced each time


def test_no_popup_without_dispatcharr_notifications(env):
    e = env()
    assert e.ps._notify_gui(e.logger, {"status": "ok", "message": "x"}) is False


def test_settings_read_failures_only_warn_when_they_last(env, caplog):
    import logging
    e = env()
    e.ps._read_settings_from_db = lambda logger: None
    e.ps._DB_WARN_AFTER_SECONDS = 0.5
    with caplog.at_level(logging.WARNING):
        p = e.ps.Plugin()
        time.sleep(0.3)
        assert "couldn't read this plugin's saved settings" not in caplog.text
        assert wait_for(lambda: "couldn't read this plugin's saved settings" in caplog.text, 2)
    p.stop(e.ctx)
