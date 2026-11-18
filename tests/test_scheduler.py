import json
import time

from conftest import wait_for, watcher_threads


def test_scheduler_only_runs_in_the_leader(env):
    """Dispatcharr creates the plugin in every process. Only on_leader_acquired may start
    anything, so creating Plugin() (or many of them) runs no test and starts no thread."""
    e = env()
    for _ in range(5):
        e.ps.Plugin()
    time.sleep(0.3)
    assert not e.runs and not watcher_threads()
    e.start_leader()
    assert wait_for(lambda: e.runs)
    assert len(watcher_threads()) == 1


def test_reload_during_a_scheduled_run_carries_on(env):
    """A reload stops the service mid-test. The test finishes, and the next leader picks up
    the same schedule from Redis instead of testing again at once."""
    e = env(run_seconds=1.0)
    p = e.start_leader()
    assert wait_for(lambda: e.runs)
    time.sleep(0.2)

    e.stop_leader(p)  # Dispatcharr waits for the thread, which finishes its test first
    assert e.redis.get(e.ps._LAST_RUN_KEY)
    assert not watcher_threads()
    p = e.start_leader()
    time.sleep(0.5)

    assert len(watcher_threads()) == 1
    assert e.status(p).startswith("Every 1 min \u00b7 next ")
    assert len(e.runs) == 1  # the hand-off didn't trigger an immediate second test


def test_two_leaders_at_once_never_test_twice(env):
    """Dispatcharr's lease can briefly overlap (or every process leads without Redis'
    lease). The shared 'a test is running' slot still allows only one test."""
    e = env(run_seconds=0.6)
    e.start_leader()
    e.start_leader()
    assert wait_for(lambda: e.runs)
    time.sleep(0.8)
    assert len(e.runs) == 1


def test_turning_the_scheduler_off_clears_it(env):
    e = env(run_seconds=0.3)
    p = e.start_leader()
    assert wait_for(lambda: e.runs)
    e.settings["scheduler_interval"] = "off"
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY) is None, 3)
    assert e.status(p).startswith("Scheduler off |")
    assert len(watcher_threads()) == 1  # the service keeps watching the setting


def test_disable_while_idle_stops_promptly(env):
    e = env()
    p = e.start_leader()
    assert wait_for(lambda: e.runs and e.redis.get(e.ps._LAST_RUN_KEY))
    started = time.time()
    e.stop_leader(p)
    assert time.time() - started < 1
    assert not watcher_threads()
    assert len(e.runs) == 1


def test_settings_read_failure_carries_on_with_the_last_settings(env, caplog):
    import logging
    e = env()
    e.ps._DB_WARN_AFTER_SECONDS = 0.5
    e.start_leader()
    assert wait_for(lambda: e.redis.get(e.ps._STATE_KEY))
    with caplog.at_level(logging.WARNING):
        e.settings_error = RuntimeError("database is down")
        time.sleep(0.3)
        assert "couldn't read this plugin's saved settings" not in caplog.text
        assert wait_for(lambda: "couldn't read this plugin's saved settings" in caplog.text, 2)
    assert e.redis.get(e.ps._STATE_KEY)  # still scheduling with the settings it had


def test_no_settings_yet_means_no_schedule(env):
    e = env()
    e.settings_error = RuntimeError("database is down")
    e.start_leader()
    time.sleep(0.4)
    assert not e.runs and e.redis.get(e.ps._STATE_KEY) is None


def test_run_slot_is_renewed_during_a_long_run(env):
    e = env(run_seconds=2.0)
    ps = e.ps
    ps._RUN_TTL = 1
    ps._KEEPALIVE_SECONDS = 0.2
    e.start_leader()
    assert wait_for(lambda: e.runs)
    time.sleep(1.5)  # past the TTL
    assert ps._s(e.redis.get(ps._RUNNING_KEY)) == ps._instance_id()
    assert wait_for(lambda: e.redis.get(ps._LAST_RUN_KEY), 3)
    assert e.redis.get(ps._RUNNING_KEY) is None  # released at the end


def test_end_run_never_deletes_someone_elses_slot(env):
    e = env()
    ps = e.ps
    assert ps._run_lock.acquire(blocking=False)
    e.redis.set(ps._RUNNING_KEY, "another-process", ex=60)
    ps._end_run(e.redis)
    assert e.redis.get(ps._RUNNING_KEY) == "another-process"


def test_busy_when_another_process_is_testing(env):
    e = env()
    e.redis.set(e.ps._RUNNING_KEY, "another-process", ex=60)
    out = e.ps._do_one_run(e.settings, e.logger, manual=True)
    assert out["status"] == "busy" and not e.runs


def test_run_now_warns_when_someone_is_watching(env):
    e = env()
    e.redis.sadd("live:channel:abc:clients", "client-1")
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    assert out["status"] == "ok"
    assert out["message"].endswith("| \u25b6\ufe0e 1 active session")
    assert len(e.runs) == 1  # Run Now still runs


def test_scheduled_skip_is_recorded_for_prometheus(env):
    e = env()
    e.settings.update({"skip_if_streaming": True, "prometheus_enabled": True})
    e.redis.sadd("live:channel:abc:clients", "client-1")
    out = e.ps._do_one_run(e.settings, e.logger, manual=False)
    assert out["status"] == "skipped" and not e.runs
    state = json.loads(e.redis.get(e.ps._METRICS_KEY))
    assert state["last_skip_timestamp_seconds"] > 0
    assert "last_success_timestamp_seconds" not in state


def test_prometheus_publish_keeps_last_good_result_on_failure(env):
    e = env()
    e.settings["prometheus_enabled"] = True
    assert e.ps._do_one_run(e.settings, e.logger, manual=True)["status"] == "ok"
    good = json.loads(e.redis.get(e.ps._METRICS_KEY))
    assert e.ps._publish_metrics(e.logger, ok=False)
    after = json.loads(e.redis.get(e.ps._METRICS_KEY))
    assert after["last_attempt_success"] == 0
    assert after["result"] == good["result"]
    assert after["last_success_timestamp_seconds"] == good["last_success_timestamp_seconds"]


def test_status_shows_exit_ip_and_metrics_leave_it_out(env):
    e = env()
    e.settings["prometheus_enabled"] = True
    p = e.ps.Plugin()
    out = p.run("run_now", {}, e.ctx)
    assert out["status"] == "ok"
    assert "(manual) \u2705 \u2b07 912.4 / \u2b06 41.8 Mbps" in e.status(p)
    metrics = json.loads(e.redis.get(e.ps._METRICS_KEY))
    assert "exit_ip" not in metrics["result"]
    assert metrics["result"]["result_url"].startswith("https://www.speedtest.net/")


def test_popup_discord_and_session_suffixes(env, monkeypatch):
    e = env()
    e.settings["discord_webhook_url"] = "https://discord.example/webhook"
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: True)
    e.redis.sadd("live:channel:a:clients", "client-1")
    e.redis.sadd("live:channel:b:clients", "client-2")
    msg = e.ps._do_one_run(e.settings, e.logger, manual=True)["message"]
    assert msg.endswith("| Source: 203.0.113.45 | \u25b6\ufe0e 2 active sessions | Discord ✓")
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: False)
    e.redis.delete("live:channel:a:clients", "live:channel:b:clients")
    msg = e.ps._do_one_run(e.settings, e.logger, manual=True)["message"]
    assert msg.endswith("| Source: 203.0.113.45 | Discord ✗ (see log)")


def test_a_second_scheduler_stands_by(env, caplog):
    """If two schedulers run at once (Dispatcharr's lease overlapping, every process leading
    without Redis, or a 1.x watcher left running after an update), only the one holding the
    scheduler lock schedules tests; the other stands by and doesn't touch the shared state."""
    import logging
    e = env(run_seconds=0.2)
    e.start_leader()
    assert wait_for(lambda: e.runs and e.redis.get(e.ps._LAST_RUN_KEY))
    first_leader = e.redis.get(e.ps._LEADER_KEY)
    with caplog.at_level(logging.INFO):
        e.start_leader()
        assert wait_for(lambda: "standing by" in caplog.text, 3)
    time.sleep(0.5)
    assert e.redis.get(e.ps._LEADER_KEY) == first_leader
    assert len(e.runs) == 1  # the second scheduler didn't start its own schedule
    assert e.status(e.ps.Plugin()).startswith("Every 1 min · next ")


def test_lock_is_handed_over_when_the_leader_stops(env):
    e = env()
    p = e.start_leader()
    assert wait_for(lambda: e.redis.get(e.ps._LEADER_KEY))
    e.stop_leader(p)
    assert e.redis.get(e.ps._LEADER_KEY) is None  # released, so the next leader starts at once
    assert e.redis.get(e.ps._STATE_KEY)  # but the schedule is kept for it


def test_lock_is_renewed_during_a_long_scheduled_run(env):
    e = env(run_seconds=2.0)
    ps = e.ps
    ps._LEADER_TTL = 1
    ps._KEEPALIVE_SECONDS = 0.2
    e.start_leader()
    assert wait_for(lambda: e.runs)
    time.sleep(1.5)  # past the lock's TTL
    assert e.redis.get(ps._LEADER_KEY)
