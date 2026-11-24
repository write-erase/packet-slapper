"""2.2: bufferbloat grade and buffering from Dispatcharr's event log."""
import json
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from conftest import FAKE_RESULT, load_plugin

ps = load_plugin()


# --- Bufferbloat grade ---

@pytest.mark.parametrize("idle, down, up, grade", [
    (10, 14, 12, "A+"),     # +4
    (10, 35, 20, "A"),      # +25
    (10, 60, 20, "B"),      # +50
    (14, 150, 17, "C"),     # +136
    (14, 250, 17, "D"),     # +236
    (14, 600, 17, "F"),     # +586
    (14, 9, 8, "A+"),       # loaded below idle counts as no rise
    (10, 20, 300, "D"),     # upload is worse, so it decides
])
def test_grade_scale(idle, down, up, grade):
    assert ps._bufferbloat({"latency_ms": idle, "download_latency_ms": down, "upload_latency_ms": up})[0] == grade


def test_no_grade_without_loaded_ping():
    assert ps._bufferbloat({"latency_ms": 10, "download_latency_ms": None, "upload_latency_ms": None}) == (None, None)


def test_grade_in_summary_popup_log_and_status_detail():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    assert s["bufferbloat_grade"] == "C" and s["bufferbloat_ms"] == 111.3
    assert "| Loaded 45/120 ms | Bloat C |" in ps._format_popup_message(s)
    assert "| Bufferbloat C (+111 ms) |" in ps._format_ui_message(s)
    assert "> Bufferbloat: C (+111 ms)" in ps._format_discord_message(s)


def test_grade_alert(env):
    e = env()
    e.settings["alert_bufferbloat_worse_than"] = "B"
    out = e.ps._do_one_run(e.settings, e.logger, manual=True)
    assert "Bloat C" in out["alerts"]
    e.settings["alert_bufferbloat_worse_than"] = "C"
    out = e.ps._do_one_run(e.settings, e.logger, manual=True)
    assert out["alerts"] == []
    e.settings["alert_bufferbloat_worse_than"] = "off"
    assert e.ps._do_one_run(e.settings, e.logger, manual=True)["alerts"] == []


def test_status_shows_the_grade(env):
    e = env()
    e.ps._do_one_run(e.settings, e.logger, manual=True)
    assert "⬇ 912.4 / ⬆ 41.8 Mbps · bloat C" in e.status(e.ps.Plugin())


# --- Buffering from Dispatcharr's event log ---

NOW = datetime(2026, 11, 24, 18, 0, tzinfo=timezone.utc)


def fake_events(monkeypatch, events):
    """Stand-ins for core.models.SystemEvent and django.utils.timezone. events: [(minutes ago, name)]."""
    rows = [(NOW - timedelta(minutes=m), name) for m, name in events]

    class Query:
        def __init__(self, **kw):
            self.kw = kw

        def values_list(self, field, flat=False):
            assert self.kw["event_type"] == "channel_buffering" and field == "channel_name" and flat
            return [name for when, name in rows if when >= self.kw["timestamp__gte"]]

    models = types.ModuleType("core.models")
    models.SystemEvent = types.SimpleNamespace(objects=types.SimpleNamespace(filter=lambda **kw: Query(**kw)))
    dj = types.ModuleType("django.utils")
    dj.timezone = types.SimpleNamespace(now=lambda: NOW)
    for name, mod in {"core": types.ModuleType("core"), "core.models": models,
                      "django": types.ModuleType("django"), "django.utils": dj}.items():
        monkeypatch.setitem(sys.modules, name, mod)


def test_recent_buffering_counts_per_channel(monkeypatch):
    fake_events(monkeypatch, [(5, "ESPN"), (10, "CNN"), (20, "ESPN"), (90, "ESPN"), (30, None)])
    total, channels = ps._recent_buffering(ps.logging.getLogger("t"))
    assert total == 4  # the one 90 minutes ago is outside the hour
    assert channels == [("ESPN", 2), ("CNN", 1), ("Unknown channel", 1)]
    assert ps._fmt_buffering(total, channels) == "4× in the last hour (ESPN 2, CNN 1, Unknown channel 1)"
    assert ps._fmt_buffering(5, [("A", 2), ("B", 1), ("C", 1), ("D", 1)]) == "5× in the last hour (A 2, B 1, C 1, +1 more)"


def test_no_event_log_means_nothing_shown(env):
    e = env()
    assert e.ps._recent_buffering(e.logger) is None
    assert "uffering" not in e.status(e.ps.Plugin())


def test_status_and_result_show_recent_buffering(env, monkeypatch):
    e = env()
    fake_events(monkeypatch, [(5, "ESPN"), (10, "ESPN"), (15, "CNN")])
    out = e.ps._do_one_run(e.settings, e.logger, manual=True)
    assert out["message"].endswith("| Buffering 3× in the last hour")
    assert out["result"]["buffering"] == "3× in the last hour (ESPN 2, CNN 1)"
    assert "| Buffering 3× in the last hour (ESPN 2, CNN 1)" in e.status(e.ps.Plugin())
    embed = e.ps._discord_embed(out["result"])["embeds"][0]
    assert {"name": "\U0001f4fa Buffering", "value": "3× in the last hour (ESPN 2, CNN 1)", "inline": False} in embed["fields"]


def test_status_says_when_there_was_no_buffering(env, monkeypatch):
    e = env()
    fake_events(monkeypatch, [(90, "ESPN")])
    assert "| No buffering in the last hour" in e.status(e.ps.Plugin())


def test_buffering_stays_out_of_prometheus(env, monkeypatch):
    e = env()
    e.settings["prometheus_enabled"] = True
    fake_events(monkeypatch, [(5, "ESPN")])
    e.ps._do_one_run(e.settings, e.logger, manual=True)
    assert "buffering" not in json.loads(e.redis.get(e.ps._METRICS_KEY))["result"]


def test_buffering_alert_posts_once_an_hour(env, monkeypatch):
    e = env()
    posts = []
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: posts.append(payload) or True)
    e.settings.update({"discord_webhook_url": "https://discord.example/x", "alert_buffering_per_hour": 3})
    e.ps._do_one_run(e.settings, e.logger, manual=True)  # a last speedtest to quote
    posts.clear()
    fake_events(monkeypatch, [(5, "ESPN"), (10, "ESPN")])
    assert not e.ps._check_buffering_alert(e.settings, e.logger, e.redis)  # 2 is under the limit
    fake_events(monkeypatch, [(5, "ESPN"), (10, "ESPN"), (15, "CNN")])
    assert e.ps._check_buffering_alert(e.settings, e.logger, e.redis)
    embed = posts[0]["embeds"][0]
    assert embed["title"] == "⚠ Buffering in Dispatcharr"
    assert embed["description"].startswith("3× in the last hour (ESPN 2, CNN 1)\n\nLast speedtest ")
    assert "⬇ 912.4 / ⬆ 41.8 Mbps · bloat C" in embed["description"]
    assert not e.ps._check_buffering_alert(e.settings, e.logger, e.redis)  # not again within the hour
    assert len(posts) == 1


def test_buffering_alert_off_by_default(env, monkeypatch):
    e = env()
    fake_events(monkeypatch, [(m, "ESPN") for m in range(1, 30)])
    assert not e.ps._check_buffering_alert(e.settings, e.logger, e.redis)


def test_buffering_alert_suggests_a_test_when_the_last_one_is_old(env, monkeypatch):
    e = env()
    posts = []
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: posts.append(payload) or True)
    e.settings.update({"discord_webhook_url": "https://discord.example/x", "discord_style": "plain",
                       "alert_buffering_per_hour": 1})
    fake_events(monkeypatch, [(5, "ESPN")])
    assert e.ps._check_buffering_alert(e.settings, e.logger, e.redis)
    assert posts[0] == ("⚠ **Buffering in Dispatcharr:** 1× in the last hour (ESPN 1)\n"
                        "No speedtest yet; Run speedtest now checks the connection.")


def test_scheduler_service_checks_for_buffering(env, monkeypatch):
    from conftest import wait_for
    e = env()
    posts = []
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: posts.append(payload) or True)
    e.settings.update({"scheduler_interval": "off", "discord_webhook_url": "https://discord.example/x",
                       "alert_buffering_per_hour": 2})
    fake_events(monkeypatch, [(5, "ESPN"), (6, "ESPN")])
    e.start_leader()
    assert wait_for(lambda: posts, 3)  # checked even with the scheduler off
    assert posts[0]["embeds"][0]["title"] == "⚠ Buffering in Dispatcharr"
