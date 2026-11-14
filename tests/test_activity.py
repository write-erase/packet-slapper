"""Viewer detection reads the same Redis keys Dispatcharr uses for its stream limits."""


def check(e):
    return e.ps.Plugin().run("check_streams", {}, e.ctx)["message"]


def test_nobody_watching(env):
    e = env()
    e.redis.set("vod_session_user:vod_1794573888844_1614", "1", ex=300)  # login mapping only
    e.redis.hset("vod_persistent_connection:vod_1", mapping={"active_streams": 0, "content_uuid": "x"})  # idle, kept for seek
    e.redis.set("profile_connections:3", "0")
    e.redis.sadd("live:channel:7:clients")  # empty set
    assert check(e) == "✓ No active sessions | Scheduled tests would run"
    assert e.ps._any_active_streams(e.logger) is False


def test_vod_counts_only_while_playing(env):
    e = env()
    e.redis.hset("vod_persistent_connection:vod_1", mapping={"active_streams": 1})
    assert check(e) == "\u25b6\ufe0e 1 active session (1 VOD) | Scheduled tests would be skipped"
    e.redis.hset("vod_persistent_connection:vod_1", "active_streams", 0)  # playback stopped
    assert check(e).startswith("✓ No active sessions")


def test_live_catch_up_and_vod_together(env):
    e = env()
    e.redis.sadd("live:channel:7:clients", "a", "b")
    e.redis.sadd("timeshift:channel:9:clients", "c")
    e.redis.hset("timeshift:channel:9:clients:c", mapping={"user_id": "1"})  # per-client hash, not counted twice
    e.redis.hset("vod_persistent_connection:vod_2", mapping={"active_streams": 1})
    assert check(e) == "\u25b6\ufe0e 4 active sessions (2 live, 1 VOD, 1 catch-up) | Scheduled tests would be skipped"


def test_older_dispatcharr_live_keys(env):
    e = env()
    e.redis.sadd("ts_proxy:channel:7:clients", "a")
    assert check(e) == "\u25b6\ufe0e 1 active session (1 live) | Scheduled tests would be skipped"


def test_provider_counter_is_a_catch_all(env):
    e = env()
    e.redis.set("profile_connections:3", "2")
    assert check(e) == "\u25b6\ufe0e Provider connections in use | Scheduled tests would be skipped"
    assert e.ps._any_active_streams(e.logger) is True


def test_redis_unavailable(env):
    e = env(redis_on=False)
    assert "Could not read viewer info" in check(e)
