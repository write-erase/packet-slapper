"""
Packet Slapper: a speedtest plugin for Dispatcharr.

Runs scheduled or on-demand Ookla Speedtests through your Dispatcharr instance.
"""

import importlib
import json
import shutil
import logging
import os
import platform
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

# Dispatcharr's registry key is the plugin folder name, lowercased with spaces as underscores.
_PLUGIN_KEY = os.path.basename(os.path.dirname(os.path.abspath(__file__))).lower().replace(" ", "_") or "packet_slapper"
PLUGIN_DATA_DIR = os.path.join("/data/plugins", _PLUGIN_KEY)
BIN_DIR = os.path.join(PLUGIN_DATA_DIR, "bin")
SPEEDTEST_BIN = os.path.join(BIN_DIR, "speedtest")
SPEEDTEST_VERSION_FILE = os.path.join(BIN_DIR, "speedtest.version")
SPEEDTEST_CLI_VERSION = "1.2.0"  # bump if Ookla ships a newer CLI release
SPEEDTEST_TIMEOUT = 90  # seconds

# Module-level so state survives across Plugin instances (Dispatcharr may
# re-instantiate the class between calls to run()).
_controller_thread = None
_controller_stop = threading.Event()
_lock = threading.Lock()
_state = {}             # scheduler state while this process is the active scheduler
_local_last_run = {}    # fallback if Redis is unavailable
_last_settings = {}     # most recent settings seen by a button click (DB-read fallback)
_logger_holder = {"logger": None}

_PROCESS_TOKEN = uuid.uuid4().hex[:8]
_run_lock = threading.Lock()
_DEFAULT_INTERVAL_MINUTES = 60

# The scheduler is driven by the "Scheduler" setting. Dispatcharr has no
# "setting saved" hook, so one small watcher thread per process re-reads the saved
# setting every few seconds. Redis holds a lease so only one process (Dispatcharr
# can run several workers) actually runs the schedule, plus shared status.
_LEADER_KEY = "packet_slapper:scheduler:leader"
_STATE_KEY = "packet_slapper:scheduler:state"
_LAST_RUN_KEY = "packet_slapper:last_run"
_RUNNING_KEY = "packet_slapper:running"   # guards against two speedtests at once
_LEADER_TTL = 240       # seconds; longer than the slowest speedtest so the lease isn't lost mid-run
_STATE_TTL = 300
_RUN_TTL = 240
_TICK_SECONDS = 5
_BUSY_RETRY_SECONDS = 60  # retry delay when a scheduled run finds another test in progress
_STARTUP_DELAY = 10     # let Dispatcharr finish starting before the first DB read


def _arch_suffix():
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        return "x86_64"
    if machine in ("aarch64", "arm64"):
        return "aarch64"
    if machine.startswith("armv7") or machine == "armhf":
        return "armhf"
    if machine in ("i386", "i686"):
        return "i386"
    raise RuntimeError(f"Unsupported architecture for Ookla speedtest CLI: {machine}")


def _installed_cli_version():
    try:
        with open(SPEEDTEST_VERSION_FILE) as fh:
            return fh.read().strip()
    except OSError:
        return None


def _ensure_speedtest_binary(logger):
    """Download the official Ookla speedtest CLI into plugin data dir if missing or outdated."""
    if (
        os.path.isfile(SPEEDTEST_BIN)
        and os.access(SPEEDTEST_BIN, os.X_OK)
        and _installed_cli_version() == SPEEDTEST_CLI_VERSION
    ):
        return SPEEDTEST_BIN

    os.makedirs(BIN_DIR, exist_ok=True)
    arch = _arch_suffix()
    url = (
        f"https://install.speedtest.net/app/cli/"
        f"ookla-speedtest-{SPEEDTEST_CLI_VERSION}-linux-{arch}.tgz"
    )
    logger.info(f"Packet Slapper: downloading Ookla CLI from {url}")

    staged = SPEEDTEST_BIN + ".new"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tgz_path = os.path.join(tmp, "speedtest.tgz")
            with urllib.request.urlopen(url, timeout=60) as resp, open(tgz_path, "wb") as out:  # noqa: S310 - fixed Ookla CDN URL
                shutil.copyfileobj(resp, out)
            # Read the one member we need straight out of the archive instead of
            # extracting to disk, so nothing in the tarball can pick its own path.
            # It lands in the bin dir under a temp name and is swapped in below,
            # so an interrupted install never leaves a half-written executable.
            with tarfile.open(tgz_path) as tf:
                member = tf.getmember("speedtest")
                if not member.isfile():
                    raise RuntimeError("'speedtest' in the archive is not a regular file")
                src = tf.extractfile(member)
                if src is None:
                    raise RuntimeError("could not read 'speedtest' from the archive")
                try:
                    os.remove(staged)  # clear any leftover from an interrupted install
                except OSError:
                    pass
                # Owner-only: only the user Dispatcharr runs as needs to execute it.
                fd = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
                with src, os.fdopen(fd, "wb") as out:
                    shutil.copyfileobj(src, out)
        os.replace(staged, SPEEDTEST_BIN)
        with open(SPEEDTEST_VERSION_FILE + ".new", "w") as fh:
            fh.write(SPEEDTEST_CLI_VERSION)
        os.replace(SPEEDTEST_VERSION_FILE + ".new", SPEEDTEST_VERSION_FILE)
    except Exception as exc:
        try:
            os.remove(staged)
        except OSError:
            pass
        raise RuntimeError(f"Could not download the Ookla speedtest CLI ({exc}). Check that Dispatcharr can reach install.speedtest.net.") from exc

    return SPEEDTEST_BIN


def _cli_env():
    """The CLI keeps its config under $HOME; fall back to our data dir if HOME isn't writable."""
    env = dict(os.environ)
    home = env.get("HOME") or os.path.expanduser("~")
    if not (home and os.path.isdir(home) and os.access(home, os.W_OK)):
        env["HOME"] = PLUGIN_DATA_DIR
    return env


def _parse_cli_json(stdout):
    """All JSON objects the CLI printed (it can emit several lines, e.g. a log line then a result)."""
    objects = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict):
            objects.append(obj)
    if not objects:
        try:
            whole = json.loads(stdout or "")
            if isinstance(whole, dict):
                objects.append(whole)
        except ValueError:
            pass
    return objects


def _pick_result(objects):
    """Return (result, error_message); at most one is not None."""
    for obj in reversed(objects):
        if obj.get("type") == "result" or ("download" in obj and "upload" in obj):
            return obj, None
    for obj in reversed(objects):
        if obj.get("type") == "error" or obj.get("level") == "error":
            return None, str(obj.get("message") or "Unknown speedtest error")
    return None, None


def _run_speedtest(logger, server_id=None):
    """Run the CLI and return the parsed result, or raise with a readable message.

    server_id: optional Ookla server ID to pin. None/blank = let the CLI auto-pick.
    """
    binary = _ensure_speedtest_binary(logger)
    cmd = [binary, "--accept-license", "--accept-gdpr", "--format=json"]
    if server_id:
        cmd.append(f"--server-id={server_id}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SPEEDTEST_TIMEOUT, env=_cli_env())
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Speedtest timed out after {SPEEDTEST_TIMEOUT} seconds (is the VPN connection up?)") from None
    except OSError as exc:
        raise RuntimeError(f"Could not run the speedtest CLI ({exc}). Delete {BIN_DIR} to force a fresh download.") from exc

    result, error = _pick_result(_parse_cli_json(proc.stdout))
    if result is None:
        if error:
            raise RuntimeError(error)
        detail = (proc.stderr or proc.stdout or "").strip()[:200] or f"exit code {proc.returncode}"
        raise RuntimeError(f"Speedtest returned no results ({detail})")
    return result


def _clean_server_id(raw):
    """Return a validated server ID string, '' for auto-select; raise on junk."""
    value = str(raw or "").strip()
    if not value:
        return ""
    if not (value.isascii() and value.isdigit()):
        raise RuntimeError(f"Server ID must be a number (got '{value}'). Leave it blank to auto-select.")
    return value


def _num(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_loss(value, sep=""):
    """Packet loss for display; the CLI leaves it out for some servers."""
    return "N/A" if value is None else f"{value:g}{sep}%"


def _clip(text, limit):
    text = str(text)
    return text if len(text) <= limit else text[: limit - 1] + "\u2026"


def _summarize_result(result, tz_name):
    """Pull display-ready values out of the raw Ookla JSON (shared by Discord + UI)."""
    server = result.get("server") or {}
    distance = server.get("distance")

    timestamp_utc = result.get("timestamp")
    time_display = time_local = "Unknown time"
    if timestamp_utc:
        try:
            dt_utc = datetime.fromisoformat(str(timestamp_utc).replace("Z", "+00:00"))
        except Exception:
            time_local = time_display = str(timestamp_utc)
        else:
            time_utc_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            time_local = time_display = time_utc_str
            if ZoneInfo is not None:
                try:
                    dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
                    time_local = f"{dt_local.strftime('%Y-%m-%d %H:%M:%S')} {dt_local.tzname() or tz_name}"
                    time_display = f"{time_local} ({time_utc_str})"
                except Exception:
                    pass  # unknown timezone name: keep showing UTC

    ping = result.get("ping") or {}
    loss_raw = result.get("packetLoss")
    return {
        "download_mbps": round(_num((result.get("download") or {}).get("bandwidth")) / 125000, 2),
        "upload_mbps": round(_num((result.get("upload") or {}).get("bandwidth")) / 125000, 2),
        "latency_ms": round(_num(ping.get("latency")), 2),
        "jitter_ms": round(_num(ping.get("jitter")), 2),
        "packet_loss": round(_num(loss_raw), 1) if loss_raw is not None else None,
        "isp": result.get("isp") or "Unknown ISP",
        "server_name": server.get("name") or "Unknown Server",
        "server_location": server.get("location") or "Unknown Location",
        "server_country": server.get("country") or "Unknown Country",
        "server_host": server.get("host") or "Unknown host",
        "distance": f"{distance} km" if distance is not None else "N/A",
        "time_display": time_display,
        "time_local": time_local,
    }


def _format_ui_message(s):
    """Single-line summary for the Dispatcharr notification popup.

    Dispatcharr's popup collapses line breaks into spaces, so fields are
    separated with ' | ' and the numbers you care about come first.
    """
    parts = [
        f"\u2b07 {s['download_mbps']} Mbps",
        f"\u2b06 {s['upload_mbps']} Mbps",
        f"Ping {s['latency_ms']} ms (jitter {s['jitter_ms']} ms)",
        f"Loss {_fmt_loss(s['packet_loss'])}",
        f"{s['server_name']}, {s['server_location']}",
        f"ISP: {s['isp']}",
    ]
    if s["distance"] != "N/A":
        parts.append(s["distance"])
    parts.append(s["time_local"])
    return " | ".join(parts)


def _discord_style(settings):
    return "plain" if str(settings.get("discord_style") or "embed").lower() == "plain" else "embed"


def _format_discord_message(s):
    """Classic plain-text Discord message."""
    lines = [
        "**Packet Slapper Speedtest**",
        f"> Server: {s['server_name']} ({s['server_location']}, {s['server_country']})",
        f"> Host: {s['server_host']}",
        f"> ISP: {s['isp']}",
        f"> Time: {s['time_display']}",
        f"> Latency: `{s['latency_ms']} ms` (Jitter: {s['jitter_ms']} ms)",
        f"> Download: `{s['download_mbps']} Mbps`",
        f"> Upload: `{s['upload_mbps']} Mbps`",
        f"> Packet Loss: `{_fmt_loss(s['packet_loss'], ' ')}`",
    ]
    if s["distance"] != "N/A":
        lines.append(f"> Distance: {s['distance']}")
    return "\n".join(lines)


def _discord_embed(s):
    """Discord embed: speeds and latency on the first row, loss/server/ISP on the second."""
    server_lines = [_clip(s["server_name"], 100), _clip(f"{s['server_location']}, {s['server_country']}", 100), f"`{_clip(s['server_host'], 100)}`"]
    if s["distance"] != "N/A":
        server_lines.append(s["distance"])
    return {
        "embeds": [{
            "title": "Packet Slapper Speedtest",
            "color": 0x2ECC71,
            "fields": [
                {"name": "\u2b07 Download", "value": f"**{s['download_mbps']}** Mbps", "inline": True},
                {"name": "\u2b06 Upload", "value": f"**{s['upload_mbps']}** Mbps", "inline": True},
                {"name": "\U0001f3d3 Latency", "value": f"**{s['latency_ms']}** ms\nJitter {s['jitter_ms']} ms", "inline": True},
                {"name": "\U0001f4c9 Packet loss", "value": _fmt_loss(s['packet_loss'], ' '), "inline": True},
                {"name": "\U0001f4cd Server", "value": "\n".join(server_lines), "inline": True},
                {"name": "\U0001f310 ISP", "value": _clip(s["isp"], 200), "inline": True},
            ],
            "footer": {"text": _clip(s["time_display"], 200)},
        }]
    }


def _discord_result_payload(s, style):
    return _format_discord_message(s) if style == "plain" else _discord_embed(s)


def _discord_error_payload(message, style):
    if style == "plain":
        return f"\u274c Speedtest failed: {_clip(message, 1900)}"
    return {"embeds": [{"title": "\u274c Speedtest failed", "description": _clip(message, 1500), "color": 0xE74C3C}]}


def _discord_test_payload(style):
    if style == "plain":
        return "\u2705 Packet Slapper: test message."
    return {"embeds": [{"title": "\u2705 Packet Slapper", "description": "Test message: your webhook works.", "color": 0x2ECC71}]}


def _post_to_discord(webhook_url, payload, logger):
    """payload: plain string or a Discord webhook JSON dict.

    Returns True if posted, False if the post failed, None if no webhook is set.
    """
    webhook_url = (webhook_url or "").strip()
    if not webhook_url:
        logger.debug("Packet Slapper: no Discord webhook set (optional), skipping post")
        return None
    if not webhook_url.lower().startswith("https://"):
        logger.error("Packet Slapper: the Discord webhook URL must start with https://")
        return False

    body = {"content": payload} if isinstance(payload, str) else payload
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (PacketSlapper/1.0)",  # Discord's CDN rejects the default Python UA
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=15)  # noqa: S310 - user-supplied webhook, by design
        return True
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", "replace")[:200]
        except Exception:
            detail = ""
        logger.error(f"Packet Slapper: Discord rejected the post (HTTP {exc.code}) {detail}")
        return False
    except Exception as exc:
        logger.error(f"Packet Slapper: failed to post to Discord: {exc}")
        return False


def _get_redis_client(logger):
    """Get Dispatcharr's Redis client without depending on the proxy module name.

    Newer Dispatcharr versions renamed apps.proxy.ts_proxy to apps.proxy.live_proxy,
    so try the shared helper first, then both proxy module paths.
    """
    try:
        from core.utils import RedisClient  # type: ignore
        client = RedisClient.get_client()
        if client is not None:
            return client
    except Exception:
        logger.debug("Packet Slapper: core.utils.RedisClient unavailable", exc_info=True)

    for module_path in ("apps.proxy.live_proxy.server", "apps.proxy.ts_proxy.server"):
        try:
            module = importlib.import_module(module_path)
            client = getattr(module.ProxyServer.get_instance(), "redis_client", None)
            if client is not None:
                return client
        except Exception:
            logger.debug(f"Packet Slapper: could not use {module_path}", exc_info=True)
    return None


# Redis key patterns that indicate someone is watching. Live TV client sets are
# the layout I'm most sure of; VOD and catch-up key names differ between
# Dispatcharr versions, so those patterns are deliberately broad. The
# "Check Active Streams" button reports exactly which keys matched, so a
# pattern that is too broad or too narrow is easy to spot and tune.
_LIVE_PATTERNS = ("*:channel:*:clients",)
_VOD_PATTERNS = ("*vod*connection*", "*vod*session*", "*vod*client*")
_CATCHUP_PATTERNS = ("*timeshift*", "*catchup*", "*catch_up*")
# Provider connection counters cover live, VOD and catch-up alike, so they act
# as a catch-all if a session type uses key names the patterns above miss.
_PROVIDER_COUNTER_PATTERNS = ("*profile_connections*",)


def _instance_id():
    """Unique per process, even if workers are forked after this module was imported."""
    return f"{os.getpid()}-{_PROCESS_TOKEN}"


def _s(value):
    return value.decode() if isinstance(value, bytes) else value


def _key_size(client, key):
    """How many entries a session/client key holds (1 for a plain string key)."""
    key_type = _s(client.type(key))
    size_fn = {
        "set": client.scard,
        "hash": client.hlen,
        "list": client.llen,
        "zset": client.zcard,
    }.get(key_type)
    if size_fn:
        return size_fn(key)
    return 1 if key_type == "string" else 0


def _counter_value(client, key):
    try:
        return int(_s(client.get(key)) or 0)
    except (TypeError, ValueError):
        return 0


def _detect_activity(logger):
    """
    Return {category: {redis_key: count}} for everything that looks like an
    active viewer, or None if Redis could not be read.

    Reads Redis (shared by all Dispatcharr workers) rather than in-process
    state, so it gives the same answer no matter which worker runs the plugin,
    and it doesn't depend on the ts_proxy -> live_proxy module rename.
    """
    client = _get_redis_client(logger)
    if client is None:
        return None

    claimed = set()

    def collect(patterns, size_fn):
        found = {}
        for pattern in patterns:
            for raw_key in client.scan_iter(match=pattern, count=200):
                key = _s(raw_key)
                if key in claimed:
                    continue
                try:
                    count = size_fn(key)
                except Exception:
                    logger.debug(f"Packet Slapper: could not size Redis key {key}", exc_info=True)
                    continue  # one odd key shouldn't disable detection
                if count > 0:
                    found[key] = count
                    claimed.add(key)
        return found

    try:
        return {
            "live": collect(_LIVE_PATTERNS, lambda k: _key_size(client, k)),
            "vod": collect(_VOD_PATTERNS, lambda k: _key_size(client, k)),
            "catch-up": collect(_CATCHUP_PATTERNS, lambda k: _key_size(client, k)),
            "provider connections": collect(_PROVIDER_COUNTER_PATTERNS, lambda k: _counter_value(client, k)),
        }
    except Exception:
        logger.exception("Packet Slapper: failed reading viewer activity from Redis")
        return None


def _any_active_streams(logger):
    """True if live, VOD or catch-up playback is in progress.

    If viewers can't be determined, assume idle (and warn) rather than skipping
    every scheduled run forever.
    """
    activity = _detect_activity(logger)
    if activity is None:
        logger.warning("Packet Slapper: could not determine active streams; assuming idle")
        return False
    busy = {category: keys for category, keys in activity.items() if keys}
    if busy:
        logger.info(f"Packet Slapper: active viewers detected: {busy}")
    return bool(busy)


def _log_related_keys(logger, limit=60):
    """Debug aid: log Redis keys that could relate to playback, with type and TTL."""
    client = _get_redis_client(logger)
    if client is None:
        return
    seen = set()
    try:
        for pattern in ("*vod*", "*timeshift*", "*catch*", "*client*", "*connection*", "*session*"):
            for raw_key in client.scan_iter(match=pattern, count=200):
                key = _s(raw_key)
                if key in seen:
                    continue
                seen.add(key)
                if len(seen) > limit:
                    logger.info(f"Packet Slapper: [debug] more than {limit} related keys, stopping list")
                    return
                logger.info(
                    f"Packet Slapper: [debug] redis key {key} "
                    f"type={_s(client.type(key))} ttl={client.ttl(key)}"
                )
        if not seen:
            logger.info("Packet Slapper: [debug] no playback-related Redis keys found")
    except Exception:
        logger.exception("Packet Slapper: failed listing related Redis keys")


def _describe_active_streams(logger):
    """Result text for the 'Check Active Streams' button."""
    activity = _detect_activity(logger)
    if activity is None:
        return "Could not read viewer info from Redis; scheduled runs will treat this as idle. See the Dispatcharr log."

    parts = []
    for category, keys in activity.items():
        if not keys:
            parts.append(f"{category}: 0")
            continue
        items = list(keys.items())
        shown = ", ".join(f"{k}={v}" for k, v in items[:3])
        more = f" +{len(items) - 3} more" if len(items) > 3 else ""
        parts.append(f"{category}: {len(items)} ({shown}{more})")

    verdict = "a scheduled speedtest would be SKIPPED" if any(activity.values()) else "a scheduled speedtest would run"
    _log_related_keys(logger)
    return " | ".join(parts) + f" -> {verdict}."


def _execute_run(settings, logger, manual=False):
    if not manual and settings.get("skip_if_streaming", True) and _any_active_streams(logger):
        logger.info("Packet Slapper: skipping scheduled run, active streams detected")
        return {"status": "skipped", "message": "Active streams detected; run skipped"}

    tz_name = str(settings.get("display_timezone") or "America/Chicago").strip()
    webhook_url = str(settings.get("discord_webhook_url") or "").strip()
    style = _discord_style(settings)

    try:
        server_id = _clean_server_id(settings.get("server_id"))
        result = _run_speedtest(logger, server_id)
        summary = _summarize_result(result, tz_name)
    except Exception as exc:
        logger.error(f"Packet Slapper: speedtest failed: {exc}", exc_info=not isinstance(exc, RuntimeError))
        _post_to_discord(webhook_url, _discord_error_payload(str(exc), style), logger)
        return {"status": "error", "message": f"\u274c Speedtest failed: {exc}"}

    ui_message = _format_ui_message(summary)
    logger.info(f"Packet Slapper: {ui_message}")  # scheduled runs land in the log too

    posted = _post_to_discord(webhook_url, _discord_result_payload(summary, style), logger)
    if posted is True:
        ui_message += " | Posted to Discord"
    elif posted is False:
        ui_message += " | Discord post FAILED (see logs)"

    # "message" is what Dispatcharr shows in the popup; "result" carries the raw numbers.
    return {"status": "ok", "message": ui_message, "result": summary}


def _begin_run(logger):
    """Claim the single 'a speedtest is running' slot. Returns (claimed, redis_client)."""
    if not _run_lock.acquire(blocking=False):
        return False, None
    client = _get_redis_client(logger)
    if client is not None:
        try:
            if not client.set(_RUNNING_KEY, _instance_id(), nx=True, ex=_RUN_TTL):
                _run_lock.release()
                return False, None
        except Exception:
            pass  # Redis hiccup: the in-process lock still protects this worker
    return True, client


def _end_run(client):
    try:
        if client is not None and _s(client.get(_RUNNING_KEY)) == _instance_id():
            client.delete(_RUNNING_KEY)
    except Exception:
        pass
    _run_lock.release()


def _do_one_run(settings, logger, manual=False):
    """Run (or skip) one speedtest and remember the outcome for the status button.

    Blocks for the length of the speedtest (up to SPEEDTEST_TIMEOUT seconds).
    Called both by the scheduler's own background watcher thread and directly
    by run() for the 'Run Speedtest Now' button, so a click on that button
    holds the Dispatcharr web worker until the test finishes -- the same
    trade-off this plugin used for most of its life. Dispatcharr's own plugin
    docs note that blocking a worker this way can stall it, but a build that
    avoids that entirely (start now, fetch the full result later, without one
    call blocking for the whole wait) isn't possible with today's plugin
    actions, which are simple request/response calls.
    """
    claimed, client = _begin_run(logger)
    if not claimed:
        logger.info("Packet Slapper: a speedtest is already running; not starting another")
        return {"status": "busy", "message": "A speedtest is already running. Try again in a minute."}
    try:
        outcome = _execute_run(settings, logger, manual)
    finally:
        _end_run(client)
        try:
            from django.db import close_old_connections
            close_old_connections()
        except Exception:
            pass
    try:
        _record_last_run(logger, outcome, manual)
    except Exception:
        logger.debug("Packet Slapper: could not record last run", exc_info=True)
    return outcome


def _record_last_run(logger, outcome, manual):
    status = outcome.get("status")
    if status == "ok":
        r = outcome.get("result") or {}
        detail = f"{r.get('download_mbps')} Mbps down / {r.get('upload_mbps')} Mbps up"
    elif status == "skipped":
        detail = "someone was watching"
    else:
        detail = str(outcome.get("message", ""))[:120]
    record = {"at": time.time(), "status": status, "manual": bool(manual), "detail": detail}
    _local_last_run.clear()
    _local_last_run.update(record)
    _redis_set(_get_redis_client(logger), _LAST_RUN_KEY, json.dumps(record))  # no TTL: survives a stop


def _redis_set(client, key, value, ttl=None):
    if client is None:
        return False
    try:
        client.set(key, value, ex=ttl)
        return True
    except Exception:
        return False


def _redis_get_json(client, key):
    if client is None:
        return None
    try:
        raw = client.get(key)
        return json.loads(_s(raw)) if raw else None
    except Exception:
        return None


def _publish_state(client):
    _state["heartbeat_at"] = time.time()
    _redis_set(client, _STATE_KEY, json.dumps(_state), _STATE_TTL)


def _current_logger():
    return _logger_holder["logger"] or logging.getLogger("plugins.packet_slapper")


def _read_settings_from_db(logger):
    """Return (plugin_enabled, settings) from Dispatcharr's DB, or None if unavailable."""
    try:
        from apps.plugins.models import PluginConfig  # type: ignore

        try:
            row = PluginConfig.objects.filter(key=_PLUGIN_KEY).values("enabled", "settings").first()
        finally:
            try:
                from django.db import close_old_connections

                close_old_connections()
            except Exception:
                pass
        if row is None:
            return None
        return bool(row["enabled"]), (row["settings"] or {})
    except Exception:
        logger.debug("Packet Slapper: could not read plugin settings from the DB", exc_info=True)
        return None


def _acquire_leadership(client):
    """True if this process holds (or just took) the scheduler lease."""
    if client is None:
        return True  # can't coordinate without Redis; assume a single process
    try:
        if client.set(_LEADER_KEY, _instance_id(), nx=True, ex=_LEADER_TTL):
            return True
        if _s(client.get(_LEADER_KEY)) == _instance_id():
            client.set(_LEADER_KEY, _instance_id(), ex=_LEADER_TTL)  # refresh
            return True
    except Exception:
        pass
    return False


def _release_leadership(client, logger):
    _state.clear()
    if client is not None:
        try:
            if _s(client.get(_LEADER_KEY)) == _instance_id():
                client.delete(_LEADER_KEY, _STATE_KEY)
        except Exception:
            pass
    logger.info("Packet Slapper: scheduler stopped")


def _interval_minutes(settings):
    """Minutes between scheduled runs, or None when the scheduler is off.

    Reads the "Scheduler" dropdown. Installs that saved the older toggle + number
    settings (and haven't touched the dropdown) keep working via the legacy keys.
    """
    choice = settings.get("scheduler_interval")
    if choice in (None, ""):
        if not bool(settings.get("scheduler_enabled", False)):
            return None
        minutes = _num(settings.get("interval_minutes"), _DEFAULT_INTERVAL_MINUTES)
        return max(minutes if minutes > 0 else _DEFAULT_INTERVAL_MINUTES, 1.0)
    if str(choice).strip().lower() == "off":
        return None
    minutes = _num(choice, -1)
    if minutes <= 0:
        return None  # unrecognized value: fail safe (off), don't silently start a schedule
    return max(minutes, 1.0)


def _scheduler_tick(client, settings, logger, holding):
    """One pass of the schedule. Returns True while this process is the active scheduler."""
    if not _acquire_leadership(client):
        return False  # another process is running the schedule

    interval_seconds = (_interval_minutes(settings) or _DEFAULT_INTERVAL_MINUTES) * 60

    if not holding:
        # Just became active: pick up the shared schedule if a previous holder left one.
        shared = _redis_get_json(client, _STATE_KEY) or {}
        _state.clear()
        _state.update({
            "started_at": shared.get("started_at") or time.time(),
            "next_run_at": shared.get("next_run_at"),
            "pid": os.getpid(),
        })
        logger.info(f"Packet Slapper: scheduler started (every {interval_seconds / 60:g} min)")

    _state["interval_minutes"] = interval_seconds / 60
    now = time.time()
    next_at = _state.get("next_run_at")
    if next_at and next_at - now > interval_seconds:  # interval was shortened
        next_at = _state["next_run_at"] = now + interval_seconds

    if not next_at or next_at <= now:
        _state["next_run_at"] = None  # shows "test in progress"
        _publish_state(client)
        outcome = None
        try:
            outcome = _do_one_run(settings, logger, manual=False)
        except Exception:
            logger.exception("Packet Slapper: unhandled error in scheduled run")
        busy = isinstance(outcome, dict) and outcome.get("status") == "busy"
        delay = min(_BUSY_RETRY_SECONDS, interval_seconds) if busy else interval_seconds
        _state["next_run_at"] = time.time() + delay

    _publish_state(client)
    return True


def _controller_loop():
    logger = _current_logger()
    logger.info("Packet Slapper: scheduler watcher started")
    client = None
    holding = False
    warned = False
    _controller_stop.wait(_STARTUP_DELAY)
    try:
        while not _controller_stop.is_set():
            logger = _current_logger()
            try:
                if client is None:
                    client = _get_redis_client(logger)
                snapshot = _read_settings_from_db(logger)
                if snapshot is None:
                    if not warned:
                        logger.warning(
                            "Packet Slapper: can't read saved settings from the database; "
                            "using the settings from the last button click instead"
                        )
                        warned = True
                    if not _last_settings:
                        # No saved settings and no button click to fall back on:
                        # wait for the DB rather than guess what the user wants.
                        _controller_stop.wait(_TICK_SECONDS)
                        continue
                    plugin_enabled, settings = True, dict(_last_settings)
                else:
                    plugin_enabled, settings = snapshot

                if plugin_enabled and _interval_minutes(settings) is not None:
                    now_holding = _scheduler_tick(client, settings, logger, holding)
                    if holding and not now_holding:
                        _state.clear()  # another process took over the schedule; don't touch its Redis keys
                        logger.info("Packet Slapper: another process is now running the schedule")
                    holding = now_holding
                elif holding:
                    _release_leadership(client, logger)
                    holding = False
            except Exception:
                logger.exception("Packet Slapper: scheduler watcher error")
            _controller_stop.wait(_TICK_SECONDS)
    finally:
        if holding:
            _release_leadership(client, logger)
        logger.info("Packet Slapper: scheduler watcher stopped")


def _ensure_controller():
    """Start this process's watcher thread if it isn't already running."""
    global _controller_thread
    with _lock:
        if _controller_thread and _controller_thread.is_alive():
            if not _controller_stop.is_set():
                return
            _controller_thread.join(timeout=3)  # finishing a stop; let it exit first
            if _controller_thread.is_alive():
                return
        _controller_stop.clear()
        _controller_thread = threading.Thread(
            target=_controller_loop, daemon=True, name="packet-slapper-scheduler"
        )
        _controller_thread.start()


def _load_scheduler_state(logger):
    """Return (state or None, shared). shared=False means Redis was unavailable."""
    client = _get_redis_client(logger)
    if client is not None:
        try:
            raw = client.get(_STATE_KEY)
            return (json.loads(_s(raw)) if raw else None), True
        except Exception:
            logger.debug("Packet Slapper: could not read scheduler state from Redis", exc_info=True)
    alive = bool(_controller_thread and _controller_thread.is_alive() and _state)
    return (dict(_state) if alive else None), False


def _load_last_run(logger):
    client = _get_redis_client(logger)
    if client is not None:
        try:
            raw = client.get(_LAST_RUN_KEY)
            if raw:
                return json.loads(_s(raw))
        except Exception:
            pass
    return dict(_local_last_run) or None


def _fmt_time(epoch, tz_name):
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    try:
        if ZoneInfo is not None:
            dt = dt.astimezone(ZoneInfo(tz_name))
    except Exception:
        pass
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def _fmt_duration(seconds):
    seconds = max(int(seconds), 0)
    hours, rem = divmod(seconds, 3600)
    minutes = rem // 60
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m"
    return f"{seconds}s"


_LAST_RUN_ICONS = {"ok": "\u2705", "skipped": "\u23ed", "error": "\u274c"}


def _describe_scheduler(settings, logger):
    """One-line scheduler status for the popup."""
    tz_name = str(settings.get("display_timezone") or "America/Chicago").strip()
    enabled = _interval_minutes(settings) is not None
    state, shared = _load_scheduler_state(logger)

    parts = []
    if state:
        parts.append("\U0001f7e2 Scheduler RUNNING" if enabled else "\U0001f7e0 Scheduler STOPPING (turned off in settings)")
        if state.get("started_at"):
            parts.append(f"Started {_fmt_time(state['started_at'], tz_name)}")
        if state.get("interval_minutes"):
            parts.append(f"Every {state['interval_minutes']:g} min")
        if enabled:
            next_at = state.get("next_run_at")
            if not next_at:
                parts.append("Test in progress")
            elif next_at <= time.time():
                parts.append("Next run due now")
            else:
                parts.append(f"Next run {_fmt_time(next_at, tz_name)} (in {_fmt_duration(next_at - time.time())})")
    elif enabled:
        parts.append("\U0001f7e1 Scheduler STARTING (enabled in settings; begins within about 15 seconds, check the Dispatcharr log if this persists)")
    else:
        parts.append("\U0001f534 Scheduler STOPPED (choose an interval under 'Scheduler' in settings to start it)")

    last = _load_last_run(logger)
    if last:
        kind = "manual" if last.get("manual") else "scheduled"
        icon = _LAST_RUN_ICONS.get(last.get("status"), "")
        parts.append(f"Last run {_fmt_time(last['at'], tz_name)} ({kind}) {icon} {last.get('detail')}".replace("  ", " "))
    else:
        parts.append("No runs recorded yet")

    if not shared:
        parts.append("(Redis unavailable, so this only reflects this worker)")
    return " | ".join(parts)


class Plugin:
    name = "Packet Slapper"
    version = "1.0.1"
    description = "Periodic/on-demand speedtests shown in Dispatcharr and optionally posted to Discord, run through Dispatcharr's own network."

    fields = []  # defined in plugin.json; kept here only if you drop the manifest

    def __init__(self):
        _ensure_controller()  # resumes the schedule after a restart if it's enabled

    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings", {}) or {}
        logger = context.get("logger") or logging.getLogger("plugins.packet_slapper")

        _logger_holder["logger"] = logger
        _last_settings.clear()
        _last_settings.update(settings)
        _ensure_controller()

        if action == "run_now":
            return _do_one_run(settings, logger, manual=True)

        if action == "scheduler_status":
            return {"status": "ok", "message": _describe_scheduler(settings, logger)}

        if action == "check_streams":
            return {"status": "ok", "message": _describe_active_streams(logger)}

        if action == "test_webhook":
            webhook_url = settings.get("discord_webhook_url", "")
            posted = _post_to_discord(webhook_url, _discord_test_payload(_discord_style(settings)), logger)
            if posted is None:
                return {"status": "ok", "message": "No Discord webhook set. It's optional; results still show in Dispatcharr."}
            if posted:
                return {"status": "ok", "message": "Test message sent to Discord"}
            return {"status": "error", "message": "Discord post failed (check the webhook URL; see the Dispatcharr log)"}

        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context: dict):
        """Called when the plugin is disabled, deleted, or reloaded."""
        logger = context.get("logger") or logging.getLogger("plugins.packet_slapper")
        _controller_stop.set()
        _last_settings.clear()  # a disabled plugin shouldn't keep a settings fallback
        _release_leadership(_get_redis_client(logger), logger)
        logger.info("Packet Slapper: stop() called, scheduler signaled to exit")
