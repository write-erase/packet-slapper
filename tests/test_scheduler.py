import json
import time

from conftest import wait_for, watcher_threads


def test_reload_during_a_scheduled_run_keeps_the_scheduler_alive(env):
    """Dispatcharr's discovery pass calls stop() then creates a new Plugin(). If that lands
    mid-test, a new watcher must start and carry on with the same schedule."""
    e = env(run_seconds=1.0)
    p = e.ps.Plugin()
    assert wait_for(lambda: e.runs)
    time.sleep(0.2)

    p.stop(e.ctx)
    p = e.ps.Plugin()

    assert wait_for(lambda: e.redis.get(e.ps._LAST_RUN_KEY), 3)  # first run finished
    assert wait_for(lambda: e.redis.get(e.ps._LEADER_KEY) == e.ps._controller.leader_id, 3)
    assert len(watcher_threads()) == 1
    status = e.status(p)
    assert "Scheduler RUNNING" in status and "Next run" in status
    assert len(e.runs) == 1  # the hand-off didn't trigger an immediate second test


def test_rapid_reloads_during_one_run(env):
    e = env(run_seconds=1.0)
    p = e.ps.Plugin()
    assert wait_for(lambda: e.runs)
    for _ in range(5):
        p.stop(e.ctx)
        p = e.ps.Plugin()
    assert wait_for(lambda: len(watcher_threads()) == 1, 3)
    assert wait_for(lambda: e.redis.get(e.ps._LEADER_KEY) == e.ps._controller.leader_id, 3)
    assert len(e.runs) == 1
    assert "Next run" in e.status(p)


def test_turning_the_scheduler_off_clears_it(env):
    e = env(run_seconds=0.3)
    p = e.ps.Plugin()
    assert wait_for(lambda: e.runs)
    e.settings["scheduler_interval"] = "off"
    assert wait_for(lambda: e.redis.get(e.ps._LEADER_KEY) is None, 3)
    assert e.redis.get(e.ps._STATE_KEY) is None
    assert "Scheduler STOPPED" in e.status(p)


def test_disable_while_idle_stops_promptly(env):
    e = env()
    p = e.ps.Plugin()
    assert wait_for(lambda: e.runs and e.redis.get(e.ps._LAST_RUN_KEY))
    p.stop(e.ctx)
    assert wait_for(lambda: not watcher_threads(), 1)
    assert e.redis.get(e.ps._LEADER_KEY) is None
    assert len(e.runs) == 1


def test_reload_without_redis_inherits_the_schedule(env):
    e = env(redis_on=False, run_seconds=0.6)
    p = e.ps.Plugin()
    assert wait_for(lambda: e.runs)
    p.stop(e.ctx)
    p = e.ps.Plugin()
    time.sleep(1.5)
    assert len(e.runs) == 1
    assert "Next run" in e.status(p)


def test_leases_are_renewed_during_a_long_run(env):
    e = env(run_seconds=2.0)
    ps = e.ps
    ps._RUN_TTL = ps._LEADER_TTL = 1
    ps._KEEPALIVE_SECONDS = 0.2
    ps.Plugin()
    assert wait_for(lambda: e.runs)
    time.sleep(1.5)  # past both TTLs
    assert ps._s(e.redis.get(ps._RUNNING_KEY)) == ps._instance_id()
    assert e.redis.get(ps._LEADER_KEY) == ps._controller.leader_id
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
    e.redis.set("ts_proxy:channel:abc:clients", "x")
    out = e.ps.Plugin().run("run_now", {}, e.ctx)
    assert out["status"] == "ok"
    assert "⚠ 1 viewer session active" in out["message"]
    assert len(e.runs) == 1  # Run Now still runs


def test_scheduled_skip_is_recorded_for_prometheus(env):
    e = env()
    e.settings.update({"skip_if_streaming": True, "prometheus_enabled": True})
    e.redis.set("ts_proxy:channel:abc:clients", "x")
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
