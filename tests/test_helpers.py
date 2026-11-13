import json
import os

import pytest

from conftest import FAKE_RESULT, load_plugin

ps = load_plugin()
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_class_metadata_comes_from_plugin_json():
    with open(os.path.join(ROOT, "plugin.json")) as fh:
        manifest = json.load(fh)
    assert ps.Plugin.version == manifest["version"]
    assert ps.Plugin.name == manifest["name"]
    assert ps.Plugin.description == manifest["description"]
    assert manifest["version"] in ps._user_agent()


def test_plugin_json_actions_are_all_handled():
    with open(os.path.join(ROOT, "plugin.json")) as fh:
        manifest = json.load(fh)
    src = open(os.path.join(ROOT, "plugin.py")).read()
    for action in manifest["actions"]:
        assert f'action == "{action["id"]}"' in src, action["id"]


def test_parse_cli_json_takes_every_object_line():
    out = '{"type":"log","level":"info","message":"hi"}\nnot json\n' + json.dumps(FAKE_RESULT)
    objs = ps._parse_cli_json(out)
    assert len(objs) == 2
    result, error = ps._pick_result(objs)
    assert result["type"] == "result" and error is None


def test_pick_result_reports_cli_error():
    result, error = ps._pick_result([{"type": "log", "level": "error", "message": "No servers"}])
    assert result is None and error == "No servers"
    assert ps._pick_result([]) == (None, None)


def test_summarize_converts_bytes_per_second_to_mbps_and_timezone():
    s = ps._summarize_result(FAKE_RESULT, "America/Chicago")
    assert s["download_mbps"] == 912.4
    assert s["upload_mbps"] == 41.8
    assert s["packet_loss"] == 0.0
    assert s["distance"] == "12.3 km"
    assert s["time_local"].startswith("2026-11-13 05:00:00 CST")
    assert "(2026-11-13 11:00:00 UTC)" in s["time_display"]


def test_missing_packet_loss_is_not_zero():
    r = dict(FAKE_RESULT)
    del r["packetLoss"]
    s = ps._summarize_result(r, "UTC")
    assert s["packet_loss"] is None
    assert "Loss N/A" in ps._format_ui_message(s)


def test_unknown_timezone_falls_back_to_utc():
    s = ps._summarize_result(FAKE_RESULT, "Not/AZone")
    assert s["time_local"] == "2026-11-13 11:00:00 UTC"


@pytest.mark.parametrize("settings, expected", [
    ({"scheduler_interval": "off"}, None),
    ({"scheduler_interval": "60"}, 60.0),
    ({"scheduler_interval": "junk"}, None),
    ({"scheduler_interval": "-5"}, None),
    ({}, None),  # legacy, toggle never turned on
    ({"scheduler_enabled": True, "interval_minutes": 15}, 15.0),  # legacy settings still work
    ({"scheduler_enabled": True, "interval_minutes": 0}, 60.0),
])
def test_interval_minutes(settings, expected):
    assert ps._interval_minutes(settings) == expected


def test_next_versions():
    assert ps._next_versions("1.2.0") == ["1.2.1", "1.3.0", "2.0.0"]
    assert ps._next_versions("1") == ["1.0.1", "1.1.0", "2.0.0"]
    assert ps._next_versions("x") == []


def test_clean_server_id():
    assert ps._clean_server_id("  12345 ") == "12345"
    assert ps._clean_server_id("") == ""
    with pytest.raises(RuntimeError):
        ps._clean_server_id("12a")
    with pytest.raises(RuntimeError):
        ps._clean_server_id("²³")  # superscript digits pass str.isdigit()


def test_discord_payloads_stay_under_limits():
    long = "x" * 5000
    assert len(ps._discord_error_payload(long, "plain")) <= 2000
    embed = ps._discord_error_payload(long, "embed")["embeds"][0]
    assert len(embed["description"]) <= 1500
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    assert len(ps._format_discord_message(s)) < 2000
    assert len(ps._discord_embed(s)["embeds"][0]["fields"]) == 6
