"""'At set times' scheduling, and Run speedtest now as a Dispatcharr background task."""
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
                        lambda settings, logger, manual=False, progress=None, leader_id=None: outcomes.append(clock.now) or
                        {"status": e.next_status.pop(0) if e.next_status else "ok"})
    e.next_status = []
    ctrl = e.ps._Scheduler(e.get_settings, e.logger)
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
    e.ctrl = e.ps._Scheduler(e.get_settings, e.logger)
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
    p = e.start_leader()
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY))
    status = e.status(p)
    assert status.startswith("At 03:00 \u00b7 next ")
    assert "| Run at: couldn't read '3pm' (use 15:00) |" in status
    e.settings["run_at_times"] = ""
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY) is None, 3)  # the scheduler notices and stops
    assert e.status(p).startswith("Scheduler off ('Run at' has no valid times)")


class Progress:
    def __init__(self):
        self.calls = []

    def __call__(self, percent, message=None):
        self.calls.append((percent, message))


def test_run_now_reports_progress_and_returns_the_result(env):
    """Run speedtest now is an async action: Dispatcharr runs it on its task queue, shows
    report_progress() updates, then pops up the returned message (green for ok)."""
    e = env()
    progress = Progress()
    out = e.ps.Plugin().run("run_now", {}, dict(e.ctx, report_progress=progress))
    assert out["status"] == "ok" and out["outcome"] == "ok"
    assert out["message"].startswith("\u2b07 912.4 Mbps")
    assert out["result"]["download_mbps"] == 912.4
    assert [p for p, _ in progress.calls] == [5, 15, 90, 100]
    assert "(manual) \u2705 \u2b07 912.4" in e.status(e.ps.Plugin())
    assert len(e.runs) == 1


def test_run_now_without_report_progress_still_works(env):
    e = env()
    assert e.ps.Plugin().run("run_now", {}, e.ctx)["status"] == "ok"


def test_run_now_alerts_are_partial(env):
    """A tripped alert shows as a yellow (partial) result in Dispatcharr."""
    e = env()
    e.settings["alert_min_download_mbps"] = 1000
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    assert out["status"] == "partial" and out["outcome"] == "ok"
    assert out["alerts"] == ["\u2b07<1000"]
    assert "\u26a0" in out["message"]


def test_run_now_failure_is_an_error(env):
    e = env()

    def broken(logger, server_id=None, auto_update=True):
        raise RuntimeError("Speedtest timed out after 90 seconds")

    e.ps._run_speedtest = broken
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    assert out["status"] == "error"
    assert out["message"] == "\u274c Speedtest failed: Speedtest timed out after 90 seconds"


def test_run_now_while_another_test_runs_is_partial(env):
    e = env()
    e.redis.set(e.ps._RUNNING_KEY, "another-process", ex=60)
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    assert out["status"] == "partial" and out["outcome"] == "busy"
    assert out["message"] == "A speedtest is already running. Try again in a minute."
    assert not e.runs


def test_task_result_mapping():
    tr = ps._task_result
    assert tr({"status": "ok", "alerts": []})["status"] == "ok"
    assert tr({"status": "publish_error"})["status"] == "partial"
    assert tr({"status": "error", "message": "x"})["status"] == "error"


def test_results_are_json_serializable(env):
    """Dispatcharr rejects a task result it can't store as JSON (no NaN either)."""
    import json
    e = env()
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    json.dumps(out, allow_nan=False)


def test_old_dispatcharr_gets_a_clear_message(env):
    """Before 0.34, Dispatcharr passes no data_dir and never calls on_leader_acquired."""
    e = env()
    out = e.ps.Plugin().run("run_now", {}, {"settings": e.settings, "logger": e.logger})
    assert out["status"] == "error" and "needs Dispatcharr 0.34.0 or newer" in out["message"]
    assert not e.runs
