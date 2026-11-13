import json
import logging
import subprocess

import pytest

from conftest import FAKE_RESULT, Env, load_plugin

ps = load_plugin()


def test_loaded_latency_in_summary_log_and_discord():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    assert s["download_latency_ms"] == 45.2 and s["upload_latency_ms"] == 120.4
    assert "Loaded ping ⬇ 45.2 ms / ⬆ 120.4 ms" in ps._format_ui_message(s)
    embed = ps._discord_embed(s)["embeds"][0]
    assert len(embed["fields"]) == 6  # still two rows
    latency = next(f for f in embed["fields"] if "Latency" in f["name"])["value"]
    assert latency.endswith("\nLoaded ⬇ 45.2 ms ⬆ 120.4 ms")
    assert "> Loaded latency: ⬇ 45.2 ms / ⬆ 120.4 ms" in ps._format_discord_message(s)


def test_popup_has_compact_loaded_latency():
    s = ps._summarize_result(FAKE_RESULT, "UTC")
    assert "| Jitter 0.71 ms | Loaded 45/120 ms | Host:" in ps._format_popup_message(s)
    only_up = dict(FAKE_RESULT, download={"bandwidth": 1})
    assert "| Loaded \u2b06 120 ms |" in ps._format_popup_message(ps._summarize_result(only_up, "UTC"))


def test_missing_loaded_latency_is_left_out():
    r = dict(FAKE_RESULT, download={"bandwidth": 1}, upload={"bandwidth": 1, "latency": {}})
    s = ps._summarize_result(r, "UTC")
    assert s["download_latency_ms"] is None and s["upload_latency_ms"] is None
    assert "Loaded" not in ps._format_ui_message(s)
    assert "Loaded" not in ps._format_popup_message(s)
    assert "Loaded" not in json.dumps(ps._discord_embed(s))
    assert "Loaded" not in ps._format_discord_message(s)


SERVERS = {
    "type": "serverList",
    "servers": [
        {"id": 1001, "host": "a.example.net:8080", "name": "Alpha ISP", "location": "Chicago, IL", "country": "United States"},
        {"id": 1002, "host": "b.example.net:8080", "name": "Beta Fiber", "location": "Naperville, IL", "country": "United States"},
        {"id": 1003, "host": "c.example.net:8080", "name": "Gamma", "location": "Gary, IN", "country": "United States"},
        {"id": 1004, "host": "d.example.net:8080", "name": "Delta", "location": "Joliet, IL", "country": "United States"},
        {"id": 1005, "host": "e.example.net:8080", "name": "Epsilon", "location": "Aurora, IL", "country": "United States"},
    ],
}


@pytest.fixture
def cli(tmp_path, monkeypatch):
    m = Env(tmp_path).ps
    m._ensure_speedtest_binary = lambda logger, auto_update=True: "/fake/speedtest"
    calls = []

    def respond(stdout="", returncode=0, stderr=""):
        def fake_run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, returncode, stdout=stdout, stderr=stderr)
        monkeypatch.setattr(m.subprocess, "run", fake_run)

    m.respond, m.calls = respond, calls
    yield m
    if m._controller is not None:  # Plugin() starts a watcher; stop it so other tests see none
        m._controller.stop.set()
        m._controller.thread.join(timeout=3)


def test_find_servers_popup_shows_nearest_three(cli, caplog):
    cli.respond(json.dumps(SERVERS))
    with caplog.at_level(logging.INFO):
        out = cli.Plugin().run("find_servers", {}, {"settings": {"server_id": "1004"}, "logger": logging.getLogger("t")})
    assert out == {"status": "ok", "message": "Alpha ISP, Chicago, IL (1001) \u00b7 Beta Fiber, Naperville, IL (1002) \u00b7 Gamma, Gary, IN (1003) | +2 more in the log"}
    assert "--servers" in cli.calls[-1] and "--format=json" in cli.calls[-1]
    assert "1004  Delta, Joliet, IL, United States  (d.example.net:8080)  <- pinned" in caplog.text


def test_find_servers_notes_a_pinned_server_not_in_the_list(cli):
    cli.respond(json.dumps(SERVERS))
    out = cli._describe_servers({"server_id": "9999"}, logging.getLogger("t"))
    assert out["message"].endswith("| +2 more in the log | Pinned 9999 isn't in this list")


def test_find_servers_errors_are_readable(cli):
    cli.respond('{"type":"log","level":"error","message":"Cannot retrieve configuration"}', returncode=2)
    out = cli._describe_servers({}, logging.getLogger("t"))
    assert out == {"status": "error", "message": "❌ Could not list servers (Cannot retrieve configuration)"}
    cli.respond(json.dumps({"type": "serverList", "servers": []}))
    assert "returned no servers" in cli._describe_servers({}, logging.getLogger("t"))["message"]


def test_find_servers_timeout(cli, monkeypatch):
    def slow(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, 30)
    monkeypatch.setattr(cli.subprocess, "run", slow)
    assert "timed out" in cli._describe_servers({}, logging.getLogger("t"))["message"]


def test_find_servers_names_a_shared_city_once(cli):
    same_city = {"servers": [
        {"id": 22288, "name": "Hivelocity", "location": "Dallas, TX", "country": "United States"},
        {"id": 72712, "name": "Pavlov Media", "location": "Dallas, TX", "country": "United States"},
        {"id": 69099, "name": "AT&T", "location": "Dallas, TX", "country": "United States"},
        {"id": 11111, "name": "Other", "location": "Plano, TX", "country": "United States"},
    ]}
    cli.respond(json.dumps(same_city))
    out = cli._describe_servers({}, logging.getLogger("t"))
    assert out["message"] == "Dallas, TX: Hivelocity (22288) \u00b7 Pavlov Media (72712) \u00b7 AT&T (69099) | +1 more in the log"


def test_find_servers_posts_full_list_to_discord(cli, monkeypatch):
    cli.respond(json.dumps(SERVERS))
    sent = []
    monkeypatch.setattr(cli, "_post_to_discord", lambda url, payload, logger: sent.append(payload) or True)
    out = cli._describe_servers({"discord_webhook_url": "https://discord.example/x", "server_id": "1004"}, logging.getLogger("t"))
    assert out["message"].endswith("| Full list posted to Discord")
    embed = sent[0]["embeds"][0]
    assert embed["title"] == "Nearest Ookla servers"  # different cities, so each line keeps its own
    lines = embed["description"].split("\n")
    assert len(lines) == 5
    assert lines[0] == "`1001`  Alpha ISP, Chicago, IL"
    assert lines[3] == "\u2b50 `1004`  Delta, Joliet, IL (pinned)"


def test_find_servers_discord_shared_city_and_plain_style(cli, monkeypatch):
    same = {"servers": [{"id": 22288, "name": "Hivelocity", "location": "Dallas, TX"},
                        {"id": 72712, "name": "Pavlov Media", "location": "Dallas, TX"}]}
    cli.respond(json.dumps(same))
    sent = []
    monkeypatch.setattr(cli, "_post_to_discord", lambda url, payload, logger: sent.append(payload) or True)
    cli._describe_servers({"discord_webhook_url": "https://discord.example/x", "discord_style": "plain"}, logging.getLogger("t"))
    assert sent[0] == ("**Nearest Ookla servers (Dallas, TX)**\n> `22288`  Hivelocity\n> `72712`  Pavlov Media\n"
                       "Put an ID in 'Speedtest Server ID' to pin it.")


def test_find_servers_discord_failure_falls_back_to_log(cli, monkeypatch):
    cli.respond(json.dumps(SERVERS))
    monkeypatch.setattr(cli, "_post_to_discord", lambda url, payload, logger: False)
    out = cli._describe_servers({"discord_webhook_url": "https://discord.example/x"}, logging.getLogger("t"))
    assert out["message"].endswith("| Discord \u2717 (see log) | +2 more in the log")
