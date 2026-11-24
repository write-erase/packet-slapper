"""2.1: scheduled tests through Dispatcharr's task queue, DVR recordings, and exit IP changes."""
import json
import sys
import time
import types
from datetime import datetime, timedelta, timezone

from conftest import FAKE_RESULT, wait_for


# --- Scheduled runs go through the task queue (and so into the run history) ---

def test_scheduled_test_is_queued_as_a_scheduled_run_now(env):
    e = env()
    queued = []
    e.start_leader(dispatch=lambda action, params: queued.append((action, params)))
    assert wait_for(lambda: queued)
    assert queued[0] == ("run_now", {"scheduled": True})
    assert not e.runs  # the scheduler itself didn't run the test
    time.sleep(0.3)
    assert len(queued) == 1  # and it waits a full interval before the next one


def test_queued_scheduled_run_counts_as_scheduled(env):
    e = env()
    out = e.ps.Plugin().run("run_now", {"scheduled": True}, e.ctx)
    assert out["status"] == "ok"
    assert "(scheduled) ✅" in e.status(e.ps.Plugin())


def test_queued_scheduled_run_still_skips_for_viewers(env):
    e = env()
    e.settings["skip_if_streaming"] = True
    e.redis.sadd("live:channel:abc:clients", "client-1")
    out = e.ps.Plugin().run("run_now", {"scheduled": True}, e.ctx)
    assert out["outcome"] == "skipped" and not e.runs


def test_scheduler_skips_before_queueing_when_someone_watches(env):
    e = env()
    e.settings["skip_if_streaming"] = True
    e.redis.sadd("live:channel:abc:clients", "client-1")
    queued = []
    e.start_leader(dispatch=lambda action, params: queued.append(action))
    assert wait_for(lambda: e.redis.get(e.ps._LAST_SKIP_KEY))
    assert not queued and not e.runs


def test_scheduler_waits_when_a_test_is_already_running(env):
    e = env()
    e.redis.set(e.ps._RUNNING_KEY, "another-process", ex=60)
    queued = []
    e.start_leader(dispatch=lambda action, params: queued.append(action))
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY))
    time.sleep(0.3)
    assert not queued
    state = json.loads(e.redis.get(e.ps._STATE_KEY))
    assert state["next_run_at"] - time.time() <= 60  # retries in a minute, not an interval


def test_scheduler_runs_the_test_itself_if_queueing_fails(env):
    e = env()

    def broken(action, params):
        raise RuntimeError("no worker for the plugins queue")

    e.start_leader(dispatch=broken)
    assert wait_for(lambda: e.runs and e.redis.get(e.ps._LAST_RUN_KEY))


def test_status_shows_a_scheduled_test_in_progress(env):
    e = env()
    assert e.ps._begin_run(e.logger, manual=False)[0]
    assert "Scheduled test in progress (started " in e.status(e.ps.Plugin())
    e.ps._end_run(e.redis)


# --- DVR recordings ---

def fake_recordings(monkeypatch, rows):
    """Stand-ins for apps.channels.models.Recording and django.utils.timezone."""
    now = datetime(2026, 11, 23, 20, 0, tzinfo=timezone.utc)
    seen = {}

    class Query:
        def __init__(self, **kw):
            seen.update(kw)

        def values(self, *fields):
            lead = seen["start_time__lte"]
            return [r for r in rows if r["start"] <= lead and r["end"] >= seen["end_time__gte"]]

    models = types.ModuleType("apps.channels.models")
    models.Recording = types.SimpleNamespace(objects=types.SimpleNamespace(filter=lambda **kw: Query(**kw)))
    dj = types.ModuleType("django.utils")
    dj.timezone = types.SimpleNamespace(now=lambda: now)
    for name, mod in {"apps": types.ModuleType("apps"), "apps.channels": types.ModuleType("apps.channels"),
                      "apps.channels.models": models, "django": types.ModuleType("django"),
                      "django.utils": dj}.items():
        monkeypatch.setitem(sys.modules, name, mod)
    return now


def test_recordings_in_progress_or_starting_soon_count_as_busy(env, monkeypatch):
    e = env()
    now = datetime(2026, 11, 23, 20, 0, tzinfo=timezone.utc)
    rows = [
        {"id": 1, "start": now - timedelta(minutes=30), "end": now + timedelta(minutes=30), "custom_properties": {"status": "recording"}},
        {"id": 2, "start": now + timedelta(minutes=2), "end": now + timedelta(hours=1), "custom_properties": {"status": "scheduled"}},
        {"id": 3, "start": now + timedelta(minutes=20), "end": now + timedelta(hours=1), "custom_properties": {}},
        {"id": 4, "start": now - timedelta(minutes=30), "end": now + timedelta(minutes=30), "custom_properties": {"status": "interrupted"}},
    ]
    fake_recordings(monkeypatch, rows)
    for r in rows:
        r.setdefault("custom_properties", {})
    assert sorted(e.ps._active_recordings(e.logger)) == [1, 2]
    assert e.ps._any_active_streams(e.logger)
    msg = e.ps.Plugin().run("check_streams", {}, e.ctx)["message"]
    assert msg == "● 2 recordings now or starting soon | Scheduled tests would be skipped"


def test_scheduled_test_skips_during_a_recording(env, monkeypatch):
    e = env()
    e.settings["skip_if_streaming"] = True
    now = datetime(2026, 11, 23, 20, 0, tzinfo=timezone.utc)
    fake_recordings(monkeypatch, [{"id": 7, "start": now, "end": now + timedelta(hours=1), "custom_properties": {"status": "recording"}}])
    out = e.ps._do_one_run(e.settings, e.logger, manual=False)
    assert out["status"] == "skipped" and not e.runs


def test_recording_is_not_counted_as_a_viewer(env, monkeypatch):
    e = env()
    now = datetime(2026, 11, 23, 20, 0, tzinfo=timezone.utc)
    fake_recordings(monkeypatch, [{"id": 7, "start": now, "end": now + timedelta(hours=1), "custom_properties": {}}])
    e.redis.sadd("live:channel:abc:clients", "client-1")
    msg = e.ps.Plugin().run("check_streams", {}, e.ctx)["message"]
    assert msg == "▶︎ 1 active session (1 live) | ● 1 recording now or starting soon | Scheduled tests would be skipped"


def test_no_dvr_schedule_means_no_recordings(env):
    e = env()
    assert e.ps._active_recordings(e.logger) == []  # Dispatcharr's models aren't importable here


# --- Exit IP changes ---

def run_with_ip(e, ip, **settings):
    result = dict(FAKE_RESULT, interface=dict(FAKE_RESULT["interface"], externalIp=ip))
    e.ps._run_speedtest = lambda logger, server_id=None, auto_update=True: result
    e.settings.update(settings)
    return e.ps._do_one_run(e.settings, e.logger, manual=True)


def test_exit_ip_change_is_pointed_out(env):
    e = env()
    first = run_with_ip(e, "203.0.113.45")
    assert "New exit IP" not in first["message"]  # nothing to compare with yet
    same = run_with_ip(e, "203.0.113.45")
    assert "New exit IP" not in same["message"]
    changed = run_with_ip(e, "198.51.100.7")
    assert "| New exit IP (was 203.0.113.45)" in changed["message"]
    assert changed["result"]["exit_ip_was"] == "203.0.113.45"


def test_exit_ip_change_in_discord_hides_ips_unless_allowed(env, monkeypatch):
    e = env()
    posts = []
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: posts.append(payload) or True)
    run_with_ip(e, "203.0.113.45", discord_webhook_url="https://discord.example/x")
    run_with_ip(e, "198.51.100.7")
    isp = next(f for f in posts[-1]["embeds"][0]["fields"] if "ISP" in f["name"])["value"]
    assert isp.endswith("\U0001f504 new exit IP since the last test")
    assert "203.0.113.45" not in json.dumps(posts[-1]) and "198.51.100.7" not in json.dumps(posts[-1])
    run_with_ip(e, "192.0.2.9", discord_include_exit_ip=True)
    isp = next(f for f in posts[-1]["embeds"][0]["fields"] if "ISP" in f["name"])["value"]
    assert "`192.0.2.9`" in isp and "(was `198.51.100.7`)" in isp


def test_previous_exit_ip_stays_out_of_prometheus(env):
    e = env()
    run_with_ip(e, "203.0.113.45", prometheus_enabled=True)
    run_with_ip(e, "198.51.100.7")
    result = json.loads(e.redis.get(e.ps._METRICS_KEY))["result"]
    assert "exit_ip" not in result and "exit_ip_was" not in result


def test_exit_ip_change_survives_a_dispatcharr_restart(env):
    """A new exit IP usually comes with a Gluetun restart, which often restarts Dispatcharr
    and empties its Redis. The data folder still remembers the last exit IP."""
    e = env()
    run_with_ip(e, "95.173.217.202")
    e.redis.d.clear()                      # Dispatcharr's Redis starts empty
    e.ps._local_last_exit_ip.clear()       # and so does the new process
    changed = run_with_ip(e, "159.26.100.17")
    assert "| New exit IP (was 95.173.217.202)" in changed["message"]


def test_exit_ip_memory_without_redis(env):
    e = env(redis_on=False)
    run_with_ip(e, "203.0.113.45")
    e.ps._local_last_exit_ip.clear()
    assert "New exit IP (was 203.0.113.45)" in run_with_ip(e, "198.51.100.7")["message"]
