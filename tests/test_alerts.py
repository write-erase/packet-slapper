"""VPN leak and threshold alerts, and when Discord gets posted to."""
import pytest

from conftest import FAKE_RESULT

HOOK = "https://discord.example/webhook"


@pytest.fixture
def run(env, monkeypatch):
    """run(**settings, result=..., fail=...) -> (outcome, posts made during that run)."""
    e = env()
    posts = []
    monkeypatch.setattr(e.ps, "_post_to_discord", lambda url, payload, logger: posts.append(payload) or True)
    e.settings["discord_webhook_url"] = HOOK

    def go(result=None, fail=None, **settings):
        e.settings.update(settings)

        def fake(logger, server_id=None, auto_update=True):
            if fail:
                raise RuntimeError(fail)
            return dict(result or FAKE_RESULT)

        monkeypatch.setattr(e.ps, "_run_speedtest", fake)
        before = len(posts)
        out = e.ps._do_one_run(e.settings, e.logger, manual=True)
        return out, posts[before:]

    go.env = e
    return go


def embed(payload):
    return payload["embeds"][0]


def test_no_alert_settings_means_no_alerts(run):
    out, posts = run()
    assert "⚠" not in out["message"] and "✅" not in out["message"]
    assert embed(posts[0])["title"] == "Packet Slapper Speedtest"
    assert "description" not in embed(posts[0])


VPN = dict(FAKE_RESULT, interface={"internalIp": "10.2.0.2", "name": "tun0", "isVpn": True, "externalIp": "95.173.217.17"})
NO_VPN = FAKE_RESULT  # eth0, isVpn false


def test_vpn_auto_alerts_once_a_vpn_was_seen(run):
    out, _ = run(result=NO_VPN)
    assert "⚠" not in out["message"]  # never had a VPN: no alert
    out, _ = run(result=VPN)
    assert "⚠" not in out["message"]
    out, posts = run(result=NO_VPN)
    assert "| ⚠ VPN off" in out["message"]
    e = embed(posts[0])
    assert e["title"] == "⚠ Packet Slapper Speedtest" and e["color"] == 0xE74C3C
    assert e["description"] == ("⚠ VPN not in use: this test went out through eth0, not a VPN tunnel. "
                                "Earlier tests went through tun0.")
    status = run.env.status(run.env.ps.Plugin())
    assert "⚠ Alert since" in status and ": VPN off" in status
    out, posts = run(result=VPN)
    assert "✅ Back to normal" in out["message"]
    assert embed(posts[0])["description"] == "✅ Back to normal (VPN off)"


def test_vpn_always_alerts_without_history(run):
    out, _ = run(result=NO_VPN, vpn_check="always")
    assert "| ⚠ VPN off" in out["message"]


def test_vpn_off_never_alerts_and_forgets(run):
    run(result=VPN)
    out, _ = run(result=NO_VPN, vpn_check="off")
    assert "⚠" not in out["message"]
    assert run.env.ps._vpn_seen(run.env.logger) is None
    out, _ = run(result=NO_VPN, vpn_check="auto")
    assert "⚠" not in out["message"]  # Auto starts over


def test_vpn_detected_by_name_or_flag_and_unknown_is_ignored(run):
    ps = run.env.ps
    assert ps._is_vpn_interface({"name": "wg0"}) is True
    assert ps._is_vpn_interface({"name": "eth0", "isVpn": True}) is True
    assert ps._is_vpn_interface({"name": "eth0", "isVpn": False}) is False
    assert ps._is_vpn_interface({}) is None
    run(result=VPN)
    unknown = {k: v for k, v in FAKE_RESULT.items() if k != "interface"}
    out, _ = run(result=unknown)
    assert "⚠" not in out["message"]  # can't tell, so no alert


def test_vpn_memory_survives_restart(run):
    run(result=VPN)
    run.env.ps._local_vpn_seen.clear()  # as after a restart; Redis still has it
    out, _ = run(result=NO_VPN)
    assert "VPN off" in out["message"]


@pytest.mark.parametrize("settings, result_change, short, detail", [
    ({"alert_min_download_mbps": 1000}, {}, "⬇<1000", "Download 912.4 Mbps is below your 1000 Mbps alert."),
    ({"alert_min_upload_mbps": "50"}, {}, "⬆<50", "Upload 41.8 Mbps is below your 50 Mbps alert."),
    ({"alert_max_loss_pct": 1}, {"packetLoss": 2.5}, "Loss>1%", "Packet loss 2.5% is above your 1% alert."),
    ({"alert_max_loaded_latency_ms": 30}, {}, "Loaded>30ms",
     "Loaded ping ⬇ 45.2 ms is above your 30 ms alert (bufferbloat, a common cause of buffering)."),
])
def test_thresholds(run, settings, result_change, short, detail):
    out, posts = run(result=dict(FAKE_RESULT, **result_change), **settings)
    assert out["message"].endswith(f"| ⚠ {short} | Discord ✓")
    e = embed(posts[0])
    assert e["color"] == 0xF39C12
    assert e["description"] == f"⚠ {detail}"


def test_thresholds_not_tripped_or_off(run):
    out, _ = run(alert_min_download_mbps=900, alert_min_upload_mbps="", alert_max_loss_pct=0,
                 alert_max_loaded_latency_ms="abc")
    assert "⚠" not in out["message"]


def test_alerts_only_mode_posts_on_start_change_and_recovery(run):
    mode = {"discord_post_mode": "alerts"}
    _, posts = run(**mode)
    assert posts == []  # nothing wrong, nothing posted
    _, posts = run(alert_min_upload_mbps=50, **mode)
    assert len(posts) == 1 and "Upload 41.8" in embed(posts[0])["description"]
    _, posts = run()
    assert posts == []  # same alert again: quiet
    _, posts = run(alert_min_download_mbps=1000)
    assert len(posts) == 1 and embed(posts[0])["description"].count("⚠") == 2  # alert set changed
    out, posts = run(alert_min_upload_mbps="", alert_min_download_mbps="")
    assert out["message"].endswith("| ✅ Back to normal | Discord ✓")
    e = embed(posts[0])
    assert e["title"] == "✅ Packet Slapper Speedtest"
    assert e["description"] == "✅ Back to normal (⬇<1000, ⬆<50)"
    _, posts = run()
    assert posts == []  # back to quiet


def test_every_mode_marks_recovery_on_the_normal_post(run):
    run(alert_min_upload_mbps=50)
    out, posts = run(alert_min_upload_mbps="")
    assert "✅ Back to normal" in out["message"]
    assert embed(posts[0])["description"] == "✅ Back to normal (⬆<50)"


def test_partial_recovery_lists_what_cleared(run):
    run(alert_min_upload_mbps=50, alert_min_download_mbps=1000)
    _, posts = run(alert_min_upload_mbps="")
    assert embed(posts[0])["description"] == (
        "⚠ Download 912.4 Mbps is below your 1000 Mbps alert.\n✅ Cleared: ⬆<50")


def test_failed_tests_alert_once_in_alerts_mode(run):
    mode = {"discord_post_mode": "alerts"}
    out, posts = run(fail="No servers", **mode)
    assert out["status"] == "error" and len(posts) == 1 and "No servers" in embed(posts[0])["description"]
    _, posts = run(fail="No servers")
    assert posts == []
    _, posts = run()
    assert embed(posts[0])["description"] == "✅ Back to normal (Test failed)"


def test_failed_tests_post_every_time_in_every_mode(run):
    assert len(run(fail="x")[1]) == 1
    assert len(run(fail="x")[1]) == 1


def test_failure_keeps_earlier_alerts(run):
    run(alert_min_download_mbps=1000, discord_post_mode="alerts")
    run(fail="timeout")
    state = run.env.ps._load_alert_state(run.env.logger)
    assert set(state["alerts"]) == {"download", "failed"}


def test_plain_style_alert_lines_come_first(run):
    _, posts = run(discord_style="plain", vpn_check="always")
    lines = posts[0].split("\n")
    assert lines[0].startswith("⚠ VPN not in use")
    assert lines[1] == "**Packet Slapper Speedtest**"


def test_alert_state_survives_in_redis(run):
    run(alert_min_download_mbps=1000)
    ps = run.env.ps
    ps._local_alert_state.clear()  # as after a restart
    assert set(ps._load_alert_state(run.env.logger)["alerts"]) == {"download"}
