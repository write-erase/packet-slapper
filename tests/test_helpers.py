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


def test_cli_lives_next_to_the_plugin_folder_not_inside_it():
    # Dispatcharr replaces the plugin folder on every update, so the CLI must live outside it,
    # and the location must follow wherever Dispatcharr's plugins directory actually is.
    assert ps.PLUGIN_DIR == ROOT
    assert ps.PLUGIN_DATA_DIR == os.path.join(os.path.dirname(ROOT), ".packet_slapper")
    assert not os.path.commonpath([ps.PLUGIN_DIR, ps.BIN_DIR]) == ps.PLUGIN_DIR
    assert ps._PLUGIN_KEY == os.path.basename(ROOT).lower().replace(" ", "_")


def test_summary_has_exit_ip_and_result_link():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    assert s["exit_ip"] == "203.0.113.45"
    assert s["result_url"] == "https://www.speedtest.net/result/c/8f2e1c4a"
    msg = ps._format_ui_message(s)
    assert "Exit IP 203.0.113.45" in msg
    assert msg.endswith("https://www.speedtest.net/result/c/8f2e1c4a")


def test_missing_or_odd_interface_and_result_are_left_out():
    r = {k: v for k, v in FAKE_RESULT.items() if k not in ("interface", "result")}
    s = ps._summarize_result(r, "UTC")
    assert s["exit_ip"] is None and s["result_url"] is None
    msg = ps._format_ui_message(s)
    assert "Exit IP" not in msg and "speedtest.net" not in msg
    assert "url" not in ps._discord_embed(s, include_ip=True)["embeds"][0]
    r2 = dict(FAKE_RESULT, result={"url": "javascript:alert(1)"})
    assert ps._summarize_result(r2, "UTC")["result_url"] is None


def test_discord_embed_ip_only_when_enabled_and_stays_two_rows():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    off = ps._discord_embed(s)["embeds"][0]
    on = ps._discord_embed(s, include_ip=True)["embeds"][0]
    assert off["url"] == on["url"] == s["result_url"]
    assert "203.0.113.45" not in json.dumps(off)
    assert len(off["fields"]) == len(on["fields"]) == 6
    isp = next(f for f in on["fields"] if "ISP" in f["name"])
    assert isp["value"] == "Example ISP\n`203.0.113.45`"


def test_discord_plain_ip_only_when_enabled():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    off = ps._format_discord_message(s)
    on = ps._format_discord_message(s, include_ip=True)
    assert "203.0.113.45" not in off
    assert "> ISP: Example ISP (`203.0.113.45`)" in on
    assert "> Result: <https://www.speedtest.net/result/c/8f2e1c4a>" in off


def test_settings_switch_controls_the_payload():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    for style in ("embed", "plain"):
        assert "203.0.113.45" not in json.dumps(ps._discord_result_payload(s, style, ps._discord_include_ip({})))
        assert "203.0.113.45" in json.dumps(ps._discord_result_payload(s, style, ps._discord_include_ip({"discord_include_exit_ip": True})))


def test_popup_is_short_and_log_line_has_everything():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    popup = ps._format_popup_message(s)
    assert popup == "⬇ 912.4 Mbps | ⬆ 41.8 Mbps | Ping 9.12 ms | Loss 0% | Jitter 0.71 ms | Host: Chicago, IL | Source: 203.0.113.45"
    full = ps._format_ui_message(s)
    for part in ("Example, Chicago, IL", "ISP: Example ISP", "12.3 km", s["result_url"]):
        assert part in full and part not in popup


def test_popup_leaves_out_unknown_host_and_missing_source():
    r = {k: v for k, v in FAKE_RESULT.items() if k not in ("interface", "server")}
    popup = ps._format_popup_message(ps._summarize_result(r, "UTC"))
    assert "Host:" not in popup and "Source:" not in popup
