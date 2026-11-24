"""
Packet Slapper: a speedtest plugin for Dispatcharr.

Runs scheduled or on-demand Ookla Speedtests through your Dispatcharr instance.

The 2.x line uses the plugin features added in Dispatcharr 0.34: the schedule runs as a
persistent service in one process Dispatcharr picks (on_leader_acquired), Run speedtest now
is an async action with progress and run history, and files live in the plugin's own data
directory. 1.3.0 is the last release for Dispatcharr older than 0.34.
"""

import hashlib
import json
import shutil
import logging
import os
import platform
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

# The folder Dispatcharr installed this plugin into, wherever its plugins directory is
# (it can be moved with DISPATCHARR_PLUGINS_DIR, so it's never hardcoded here).
PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
# Where 1.x kept the Speedtest CLI: next to the plugin folder, so updates didn't delete it.
# 2.x uses the data directory Dispatcharr gives each plugin (see _use_data_dir) and copies
# a CLI from here the first time, so updating from 1.x doesn't download it again.
_LEGACY_DATA_DIR = os.path.join(os.path.dirname(PLUGIN_DIR), ".packet_slapper")
# Until Dispatcharr hands over its data directory (in run() and on_leader_acquired), these
# point at the 1.x location. _use_data_dir() repoints all of them at once.
PLUGIN_DATA_DIR = _LEGACY_DATA_DIR
BIN_DIR = os.path.join(PLUGIN_DATA_DIR, "bin")
SPEEDTEST_BIN = os.path.join(BIN_DIR, "speedtest")
SPEEDTEST_VERSION_FILE = os.path.join(BIN_DIR, "speedtest.version")
SPEEDTEST_CHECKED_FILE = os.path.join(BIN_DIR, "speedtest.checked")  # mtime = last update check
# Known-good CLI version, used for a first install and as the floor when looking for
# newer releases. Newer releases are picked up automatically (see _ensure_speedtest_binary),
# so this never needs bumping.
SPEEDTEST_CLI_BASE_VERSION = "1.2.0"
_CLI_URL = "https://install.speedtest.net/app/cli/ookla-speedtest-{version}-linux-{arch}.tgz"
_UPDATE_CHECK_INTERVAL = 24 * 3600  # look for a newer CLI at most once a day (shared by all workers)
_CLI_CHECK_LOCK_KEY = "packet_slapper:cli_check"  # lets one worker do the startup install/check
_cli_update_checked_at = 0.0  # in-process backstop if the marker file can't be written
_install_lock = threading.Lock()
_DEFAULT_TIMEZONE = "UTC"
SPEEDTEST_TIMEOUT = 90  # seconds

# Module-level so state survives across Plugin instances (Dispatcharr may
# re-instantiate the class between calls to run()).
_active_scheduler = None  # this process's _Scheduler while it holds the schedule
_local_last_run = {}    # fallback if Redis is unavailable
_local_last_skip = {}   # same, for the skip streak
_local_alert_state = {}  # same, for active alerts
_local_vpn_seen = {}     # same, for VPN check "Auto"
_local_current_run = {}  # same, for the test in progress
_local_last_exit_ip = {}  # same, for the last exit IP

_PROCESS_TOKEN = uuid.uuid4().hex[:8]
_run_lock = threading.Lock()
_DEFAULT_INTERVAL_MINUTES = 60

# The schedule runs as this plugin's persistent service: Dispatcharr picks one process
# (on_leader_acquired) and runs _Scheduler.serve on a thread it owns. The service re-reads
# the saved settings every few seconds, since Dispatcharr has no "setting saved" hook.
# Redis holds the schedule's state, so Status works in any process and a new leader
# picks up where the last one stopped.
# Only the scheduler holding this lock schedules tests. Dispatcharr's leader election
# should already allow just one, but its lease isn't a strict lock (and without Redis every
# process leads), so a second scheduler stands by instead of running its own schedule.
_LEADER_KEY = "packet_slapper:scheduler:leader"
_STATE_KEY = "packet_slapper:scheduler:state"
_LAST_RUN_KEY = "packet_slapper:last_run"
_LAST_SKIP_KEY = "packet_slapper:last_skip"   # current streak of runs skipped because someone was watching
_RUNNING_KEY = "packet_slapper:running"   # guards against two speedtests at once
_CURRENT_RUN_KEY = "packet_slapper:current_run"  # {"manual", "at"} while a test runs, for Status
# Latest result for Prometheus, read by the Dispatcharr Exporter plugin. No TTL, so the
# last value survives restarts; the exporter turns the timestamps into staleness alerts.
_METRICS_KEY = "packet_slapper:prometheus:latest"
_METRICS_ATTEMPTS = 3
# Which alerts are currently active, so alerts-only mode posts once when a problem starts
# or changes and once when it clears, instead of every run. No TTL: survives restarts.
_ALERT_STATE_KEY = "packet_slapper:alerts"
# Set once a test has gone through a VPN tunnel: VPN check "Auto" alerts from then on.
_VPN_SEEN_KEY = "packet_slapper:vpn_seen"
# When the last "buffering keeps happening" alert went out, so it posts at most once an hour.
_BUFFERING_ALERT_KEY = "packet_slapper:buffering_alert"
_BUFFERING_WINDOW = 3600        # "the last hour"
_BUFFERING_CHECK_SECONDS = 60   # how often the scheduler service checks for the alert
_BUFFERING_MAX_EVENTS = 1000    # cap on events read per check
# The exit IP of the last successful test, so a change (a new VPN server) can be pointed out.
_LAST_EXIT_IP_KEY = "packet_slapper:last_exit_ip"
_LAST_EXIT_IP_FILE = "last_exit_ip"  # same, in the data folder: survives a Dispatcharr restart
_LEADER_TTL = 240       # seconds; longer than the slowest speedtest, and renewed during one
_STATE_TTL = 300
_RUN_TTL = 240
_TICK_SECONDS = 5
_KEEPALIVE_SECONDS = 30   # how often leases are renewed while a speedtest is running
_BUSY_RETRY_SECONDS = 60  # retry delay when a scheduled run finds another test in progress
_SKIP_RETRY_SECONDS = 1800  # retry delay after a run is skipped because someone was watching
_SKIP_HINT_AFTER = 3    # skips in a row before Status suggests checking what is being detected
_STARTUP_DELAY = 10     # let Dispatcharr finish starting before the first settings read
_DB_WARN_AFTER_SECONDS = 60  # settings reads failing this long get a warning in the log


def _user_agent():
    return f"Mozilla/5.0 (PacketSlapper/{_MANIFEST.get('version', '1')})"


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
    raise RuntimeError(f"Unsupported architecture for the Speedtest CLI: {machine}")


def _installed_cli_version():
    try:
        with open(SPEEDTEST_VERSION_FILE) as fh:
            return fh.read().strip()
    except OSError:
        return None


def _use_data_dir(context, logger):
    """Keep the CLI in the data directory Dispatcharr gives this plugin (context["data_dir"]).

    It survives plugin updates, and writing there needs no extra permission. On the first
    switch, a CLI that 1.x installed next to the plugin folder is copied over.
    """
    global PLUGIN_DATA_DIR, BIN_DIR, SPEEDTEST_BIN, SPEEDTEST_VERSION_FILE, SPEEDTEST_CHECKED_FILE
    data_dir = str((context or {}).get("data_dir") or "")
    if not data_dir or os.path.abspath(data_dir) == os.path.abspath(PLUGIN_DATA_DIR):
        return
    with _install_lock:
        PLUGIN_DATA_DIR = data_dir
        BIN_DIR = os.path.join(data_dir, "bin")
        SPEEDTEST_BIN = os.path.join(BIN_DIR, "speedtest")
        SPEEDTEST_VERSION_FILE = os.path.join(BIN_DIR, "speedtest.version")
        SPEEDTEST_CHECKED_FILE = os.path.join(BIN_DIR, "speedtest.checked")
        _adopt_legacy_cli(logger)


def _adopt_legacy_cli(logger):
    """Copy the CLI that 1.x installed (plugins/.packet_slapper/bin) into the data directory."""
    legacy_bin_dir = os.path.join(_LEGACY_DATA_DIR, "bin")
    legacy_bin = os.path.join(legacy_bin_dir, "speedtest")
    if os.path.abspath(legacy_bin_dir) == os.path.abspath(BIN_DIR):
        return
    if os.path.isfile(SPEEDTEST_BIN) or not os.path.isfile(legacy_bin):
        return
    staged = f"{SPEEDTEST_BIN}.new.{os.getpid()}"
    try:
        os.makedirs(BIN_DIR, exist_ok=True)
        with open(legacy_bin, "rb") as src, open(staged, "wb") as out:
            shutil.copyfileobj(src, out)
        os.chmod(staged, 0o700)
        os.replace(staged, SPEEDTEST_BIN)
        try:
            with open(os.path.join(legacy_bin_dir, "speedtest.version")) as fh:
                version = fh.read().strip()
        except OSError:
            version = ""
        if version:
            with open(SPEEDTEST_VERSION_FILE, "w") as fh:
                fh.write(version)
        logger.info(f"Packet Slapper: moved the Speedtest CLI into {BIN_DIR}")
    except OSError as exc:
        try:
            os.remove(staged)
        except OSError:
            pass
        logger.warning(f"Packet Slapper: could not copy the old Speedtest CLI ({exc}); it will be downloaded again")


def _version_tuple(version):
    try:
        return tuple(int(x) for x in str(version).strip().split("."))
    except ValueError:
        return None


def _next_versions(version):
    """The releases that could directly follow this one: next patch, minor and major."""
    parts = _version_tuple(version)
    if not parts:
        return []
    parts = (list(parts) + [0, 0, 0])[:3]
    major, minor, patch = parts
    return [f"{major}.{minor}.{patch + 1}", f"{major}.{minor + 1}.0", f"{major + 1}.0.0"]


def _cli_exists(version, arch):
    """True if Ookla's CDN has this CLI release (HEAD request, nothing is downloaded)."""
    req = urllib.request.Request(
        _CLI_URL.format(version=version, arch=arch),
        headers={"User-Agent": _user_agent()},
        method="HEAD",
    )
    try:
        with urllib.request.urlopen(req, timeout=15):  # noqa: S310 - fixed Ookla CDN URL
            return True
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):  # the CDN answers 403 or 404 for releases that don't exist
            return False
        raise


def _latest_cli_version(start, arch):
    """Newest release at or above `start`, found by probing the CDN for the next version numbers."""
    latest = start
    for _ in range(20):  # safety cap
        found = [v for v in _next_versions(latest) if _cli_exists(v, arch)]
        if not found:
            break
        latest = found[0]
    return latest


def _install_cli(version, arch, logger):
    """Download one CLI release and swap it in. Raises RuntimeError if anything goes wrong,
    leaving any previously installed binary untouched."""
    os.makedirs(BIN_DIR, exist_ok=True)
    url = _CLI_URL.format(version=version, arch=arch)
    logger.info(f"Packet Slapper: downloading Ookla CLI from {url}")

    staged = f"{SPEEDTEST_BIN}.new.{os.getpid()}"  # per process, so two workers can't collide
    try:
        # The temporary download sits inside the data directory too, so every file this
        # plugin writes stays where Dispatcharr allows it without the filesystem_write permission.
        with tempfile.TemporaryDirectory(dir=BIN_DIR) as tmp:
            tgz_path = os.path.join(tmp, "speedtest.tgz")
            with urllib.request.urlopen(url, timeout=60) as resp, open(tgz_path, "wb") as out:  # noqa: S310 - fixed Ookla CDN URL
                shutil.copyfileobj(resp, out)
            with open(tgz_path, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
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
                with src, open(staged, "xb") as out:
                    shutil.copyfileobj(src, out)
                # Owner-only: only the user Dispatcharr runs as needs to execute it.
                os.chmod(staged, 0o700)
        if version != SPEEDTEST_CLI_BASE_VERSION:
            # A release we haven't seen before: make sure it starts on this system before it
            # replaces the working one. If it doesn't, the old binary stays in place.
            try:
                check = subprocess.run([staged, "--version"], capture_output=True, text=True, timeout=20, env=_cli_env())
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise RuntimeError(f"the new CLI would not start ({exc})") from exc
            if check.returncode != 0:
                raise RuntimeError(f"the new CLI failed its startup check (exit code {check.returncode})")
        os.replace(staged, SPEEDTEST_BIN)
        version_tmp = f"{SPEEDTEST_VERSION_FILE}.new.{os.getpid()}"
        with open(version_tmp, "w") as fh:
            fh.write(version)
        os.replace(version_tmp, SPEEDTEST_VERSION_FILE)
    except Exception as exc:
        try:
            os.remove(staged)
        except OSError:
            pass
        hint = "" if isinstance(exc, RuntimeError) else " Check that Dispatcharr can reach install.speedtest.net."
        raise RuntimeError(f"Could not install the Speedtest CLI {version} ({exc}).{hint}") from exc
    logger.info(f"Packet Slapper: installed Ookla CLI {version} (download sha256 {digest})")


def _update_check_due():
    if _cli_update_checked_at and time.time() - _cli_update_checked_at < _UPDATE_CHECK_INTERVAL:
        return False
    try:
        return time.time() - os.path.getmtime(SPEEDTEST_CHECKED_FILE) >= _UPDATE_CHECK_INTERVAL
    except OSError:
        return True


def _mark_update_checked():
    global _cli_update_checked_at
    _cli_update_checked_at = time.time()
    try:
        os.makedirs(BIN_DIR, exist_ok=True)
        tmp = f"{SPEEDTEST_CHECKED_FILE}.new.{os.getpid()}"
        with open(tmp, "w") as fh:
            fh.write(str(int(_cli_update_checked_at)))
        os.replace(tmp, SPEEDTEST_CHECKED_FILE)
    except OSError:
        pass


def _ensure_speedtest_binary(logger, auto_update=True):
    """Make sure the Ookla CLI is installed and, if auto_update is on, at most once a day
    upgrade it to the newest release.

    The newest release is found by probing Ookla's CDN, so a new CLI is picked up with no
    change to the plugin. If the check, the download or the new CLI's startup test fails,
    the binary that's already installed keeps being used. With auto_update off, the CLI is
    only downloaded if it's missing, and always as SPEEDTEST_CLI_BASE_VERSION.
    """
    with _install_lock:  # a background pre-install and a Run Now click mustn't both install
        installed = _installed_cli_version()
        have_binary = os.path.isfile(SPEEDTEST_BIN) and os.access(SPEEDTEST_BIN, os.X_OK)
        if have_binary and (not auto_update or (installed and not _update_check_due())):
            return SPEEDTEST_BIN

        arch = _arch_suffix()
        floor = SPEEDTEST_CLI_BASE_VERSION
        if have_binary and installed and (_version_tuple(installed) or ()) > (_version_tuple(floor) or ()):
            floor = installed

        target = None
        if auto_update:
            try:
                target = _latest_cli_version(floor, arch)
            except Exception as exc:
                logger.warning(f"Packet Slapper: could not check for a newer Speedtest CLI ({exc})")
            _mark_update_checked()  # also on failure: don't hammer the CDN while it's unreachable

        if have_binary and installed and (target is None or target == installed):
            return SPEEDTEST_BIN

        attempts = [v for v in (target, floor if not have_binary else None) if v] or [floor]
        last_error = None
        for version in dict.fromkeys(attempts):  # de-duplicated, in order
            try:
                _install_cli(version, arch, logger)
                return SPEEDTEST_BIN
            except RuntimeError as exc:
                last_error = exc
                logger.warning(f"Packet Slapper: {exc}")
        if have_binary:
            logger.warning(f"Packet Slapper: keeping the installed Speedtest CLI ({installed or 'unknown version'})")
            return SPEEDTEST_BIN
        raise last_error


def _auto_update(settings):
    return bool(settings.get("cli_auto_update", True))


def _prewarm_cli(logger, settings):
    """Install (or update-check) the CLI in the background when the watcher starts, so the
    first Run Now after a restart doesn't hold the web worker through a download.

    A short Redis lock lets just one worker do it; the others find it done on their first run.
    """
    client = _get_redis_client(logger)
    if client is not None:
        try:
            if not client.set(_CLI_CHECK_LOCK_KEY, _instance_id(), nx=True, ex=600):
                return
        except Exception:
            pass
    try:
        _ensure_speedtest_binary(logger, _auto_update(settings))
    except Exception as exc:
        logger.warning(f"Packet Slapper: could not pre-install the Speedtest CLI ({exc}); will retry on the first run")


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


def _run_speedtest(logger, server_id=None, auto_update=True):
    """Run the CLI and return the parsed result, or raise with a readable message.

    server_id: optional Ookla server ID to pin. None/blank = let the CLI auto-pick.
    """
    binary = _ensure_speedtest_binary(logger, auto_update)
    cmd = [binary, "--accept-license", "--accept-gdpr", "--format=json"]
    if server_id:
        cmd.append(f"--server-id={server_id}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SPEEDTEST_TIMEOUT, env=_cli_env())
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Speedtest timed out after {SPEEDTEST_TIMEOUT} seconds (is the VPN connection up?)") from None
    except OSError as exc:
        raise RuntimeError(f"Could not run the Speedtest CLI ({exc}). Delete {BIN_DIR} to force a fresh download.") from exc

    result, error = _pick_result(_parse_cli_json(proc.stdout))
    if result is None:
        if error:
            raise RuntimeError(error)
        detail = (proc.stderr or proc.stdout or "").strip()[:200] or f"exit code {proc.returncode}"
        raise RuntimeError(f"Speedtest returned no results ({detail})")
    return result


_SERVER_LIST_TIMEOUT = 30  # seconds
_POPUP_SERVERS = 3          # the popup doesn't grow, so only the nearest few go there


def _list_servers(logger, auto_update=True):
    """The Ookla servers nearest to Dispatcharr's connection, nearest first.

    Uses the same network path as a speedtest, so through a VPN this lists servers near
    the VPN exit, which are the ones a test would actually pick. Uses no real bandwidth.
    """
    binary = _ensure_speedtest_binary(logger, auto_update)
    cmd = [binary, "--accept-license", "--accept-gdpr", "--servers", "--format=json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=_SERVER_LIST_TIMEOUT, env=_cli_env())
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Listing servers timed out after {_SERVER_LIST_TIMEOUT} seconds (is the VPN connection up?)") from None
    except OSError as exc:
        raise RuntimeError(f"Could not run the Speedtest CLI ({exc}).") from exc

    objects = _parse_cli_json(proc.stdout)
    for obj in reversed(objects):
        servers = obj.get("servers")
        if isinstance(servers, list):
            return [srv for srv in servers if isinstance(srv, dict) and srv.get("id") is not None]
    _, error = _pick_result(objects)
    detail = error or (proc.stderr or proc.stdout or "").strip()[:200] or f"exit code {proc.returncode}"
    raise RuntimeError(f"Could not list servers ({detail})")


def _server_label(srv):
    name = str(srv.get("name") or "Unknown").strip()
    location = str(srv.get("location") or "").strip()
    return f"{name}, {location}" if location else name


def _discord_servers_payload(servers, pinned, style):
    """The full server list for Discord, one server per line, pinned one starred."""
    locations = {str(srv.get("location") or "").strip() for srv in servers}
    shared = locations.pop() if len(locations) == 1 and "" not in locations else None
    lines = []
    for srv in servers:
        name = _clip(str(srv.get("name") or "Unknown").strip(), 60) if shared else _clip(_server_label(srv), 80)
        line = f"`{srv.get('id')}`  {name}"
        if pinned and str(srv.get("id")) == pinned:
            line = f"\u2b50 {line} (pinned)"
        lines.append(line)
    title = "Nearest Ookla servers" + (f" ({shared})" if shared else "")
    hint = "Put an ID in 'Speedtest server ID' to pin it."
    if pinned and not any(str(srv.get("id")) == pinned for srv in servers):
        hint = f"Pinned server {pinned} isn't in this list. " + hint
    if style == "plain":
        return _clip("\n".join([f"**{title}**", *(f"> {line}" for line in lines), hint]), 1990)
    return {"embeds": [{
        "title": title,
        "description": _clip("\n".join(lines), 3900),
        "color": 0x3498DB,
        "footer": {"text": hint},
    }]}


def _describe_servers(settings, logger):
    """Result text for the 'Find servers' button: nearest few in the popup, all of them in
    the log, and in Discord when a webhook is set (the popup has no room for more)."""
    try:
        servers = _list_servers(logger, _auto_update(settings))
    except RuntimeError as exc:
        logger.error(f"Packet Slapper: {exc}")
        return {"status": "error", "message": f"\u274c {exc}"}
    if not servers:
        return {"status": "error", "message": "\u274c Ookla returned no servers. Check that Dispatcharr can reach the internet."}

    try:
        pinned = _clean_server_id(settings.get("server_id"))
    except RuntimeError:
        pinned = ""
    logger.info(f"Packet Slapper: {len(servers)} nearest Ookla servers (put one ID in 'Speedtest server ID'):")
    for srv in servers:
        mark = "  <- pinned" if pinned and str(srv.get("id")) == pinned else ""
        logger.info(f"Packet Slapper:   {srv.get('id')}  {_server_label(srv)}, {srv.get('country') or ''}  ({srv.get('host') or ''}){mark}".rstrip())

    nearest = servers[:_POPUP_SERVERS]
    locations = {str(srv.get("location") or "").strip() for srv in nearest}
    if len(locations) == 1 and "" not in locations:
        # All in one place (the usual case): say where once, then just name (ID).
        shown = f"{locations.pop()}: " + " \u00b7 ".join(
            f"{_clip(str(srv.get('name') or 'Unknown').strip(), 30)} ({srv.get('id')})" for srv in nearest
        )
    else:
        shown = " \u00b7 ".join(f"{_clip(_server_label(srv), 40)} ({srv.get('id')})" for srv in nearest)
    more = len(servers) - _POPUP_SERVERS
    webhook_url = str(settings.get("discord_webhook_url") or "").strip()
    posted = None
    if webhook_url:
        posted = _post_to_discord(webhook_url, _discord_servers_payload(servers, pinned, _discord_style(settings)), logger)
    if posted is True:
        tail = " | Full list posted to Discord"
    elif posted is False:
        tail = " | Discord \u2717 (see log)" + (f" | +{more} more in the log" if more > 0 else "")
    else:
        tail = f" | +{more} more in the log" if more > 0 else ""
    if pinned and not any(str(srv.get("id")) == pinned for srv in servers):
        tail += f" | Pinned {pinned} isn't in this list"
    if not _save_server_list(servers, logger):
        return {"status": "ok", "message": f"{shown}{tail}"}
    # Dispatcharr only builds dropdown choices when it loads a plugin, so ask it to reload
    # Packet Slapper: get_fields() then turns 'Speedtest server' into a list of these servers.
    return {
        "status": "ok",
        "message": f"{shown}{tail} | Pick one under Speedtest server",
        "_dispatcharr": {"refresh": "plugin"},
    }


_AUTO_SERVER = "auto"
_SERVER_LIST_FILE = "servers.json"
_SERVER_LIST_MAX = 20


def _server_list_path(data_dir=None):
    return os.path.join(data_dir or PLUGIN_DATA_DIR, _SERVER_LIST_FILE)


def _save_server_list(servers, logger):
    """Keep what Find servers found, for the 'Speedtest server' dropdown."""
    keep = [
        {k: srv.get(k) for k in ("id", "name", "location", "country", "host")}
        for srv in servers[:_SERVER_LIST_MAX]
    ]
    path = _server_list_path()
    tmp = f"{path}.new.{os.getpid()}"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"found_at": time.time(), "servers": keep}, fh)
        os.replace(tmp, path)
        return True
    except OSError as exc:
        logger.warning(f"Packet Slapper: could not save the server list ({exc})")
        return False


def _load_server_list(data_dir):
    try:
        with open(_server_list_path(data_dir), encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return []
    servers = data.get("servers") if isinstance(data, dict) else None
    return [s for s in servers or [] if isinstance(s, dict) and str(s.get("id") or "").isdigit()]


def _server_field(field, servers, saved):
    """The 'Speedtest server' setting as a dropdown of the servers Find servers found."""
    auto_label = "Automatic (Ookla picks a nearby server each time)"
    options = [{"value": _AUTO_SERVER, "label": auto_label}]
    for srv in servers:
        name = str(srv.get("name") or "Unknown").strip()
        location = str(srv.get("location") or "").strip()
        label = f"{name} \u00b7 {location}" if location else name
        options.append({"value": str(srv["id"]), "label": f"{_clip(label, 60)} ({srv['id']})"})
    if saved and saved not in {o["value"] for o in options}:
        options.append({"value": saved, "label": f"Server {saved} (not in the last Find servers list)"})
    out = {k: v for k, v in field.items() if k not in ("placeholder", "type", "default")}
    out.update({
        "label": "Speedtest server",
        "type": "select",
        "default": _AUTO_SERVER,
        "placeholder": auto_label,
        "options": options,
        "help_text": "Pins tests to one Ookla server so results are comparable from run to run. "
                     "These are the servers the last 'Find servers' found; click it again after "
                     "changing VPN location. Automatic lets Ookla pick each time.",
    })
    return out


def _clean_server_id(raw):
    """Return a validated server ID string, '' for auto-select; raise on junk."""
    value = str(raw or "").strip()
    if not value or value.lower() == _AUTO_SERVER:
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


def _loaded_latency(direction):
    """Loaded latency in ms for one direction, or None if the CLI didn't report it."""
    value = ((direction or {}).get("latency") or {}).get("iqm")
    if value is None:
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None


def _fmt_loaded(s, sep=" / "):
    """'⬇ 45.2 ms / ⬆ 120.4 ms' from whichever directions were reported, or None."""
    parts = []
    if s.get("download_latency_ms") is not None:
        parts.append(f"\u2b07 {s['download_latency_ms']:g} ms")
    if s.get("upload_latency_ms") is not None:
        parts.append(f"\u2b06 {s['upload_latency_ms']:g} ms")
    return sep.join(parts) or None


# Interface names VPN software uses for its tunnel: OpenVPN (tun/tap), WireGuard (wg),
# PPP-based VPNs, IPsec, and the names some VPN apps give their WireGuard tunnel.
# Bufferbloat grade: how much ping rises while the line is busy (loaded ping minus idle
# ping, the worse of download and upload), on the scale common bufferbloat tests use.
_BLOAT_GRADES = ((5, "A+"), (30, "A"), (60, "B"), (200, "C"), (400, "D"))
_GRADE_ORDER = ("A+", "A", "B", "C", "D", "F")


def _bufferbloat(s):
    """(grade, extra_ms) from a summary, or (None, None) without loaded ping."""
    idle = s.get("latency_ms")
    loaded = [v for v in (s.get("download_latency_ms"), s.get("upload_latency_ms")) if v is not None]
    if idle is None or not loaded:
        return None, None
    extra = max(0.0, max(loaded) - idle)
    for limit, grade in _BLOAT_GRADES:
        if extra <= limit:
            return grade, extra
    return "F", extra


def _fmt_bloat(s):
    """'C (+136 ms)', or None."""
    grade, extra = s.get("bufferbloat_grade"), s.get("bufferbloat_ms")
    if not grade:
        return None
    return f"{grade} (+{extra:.0f} ms)"


_VPN_INTERFACE_PREFIXES = ("tun", "tap", "wg", "ppp", "ipsec", "vti", "nordlynx", "proton", "mullvad")


def _is_vpn_interface(interface):
    """True/False for whether a test went through a VPN tunnel, None when unknown."""
    name = str(interface.get("name") or "").strip().lower()
    if interface.get("isVpn") is True or (name and name.startswith(_VPN_INTERFACE_PREFIXES)):
        return True
    if name or interface.get("isVpn") is False:
        return False
    return None


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
                    utc_short = dt_utc.strftime("%H:%M:%S UTC") if dt_local.date() == dt_utc.date() else time_utc_str
                    time_display = f"{time_local} ({utc_short})"
                except Exception:
                    pass  # unknown timezone name: keep showing UTC

    ping = result.get("ping") or {}
    loss_raw = result.get("packetLoss")
    interface = result.get("interface") or {}
    exit_ip = str(interface.get("externalIp") or "").strip() or None
    result_url = str((result.get("result") or {}).get("url") or "").strip()
    if not result_url.startswith("https://"):
        result_url = None  # only ever link to a real https result page
    summary = {
        "download_mbps": round(_num((result.get("download") or {}).get("bandwidth")) / 125000, 2),
        "upload_mbps": round(_num((result.get("upload") or {}).get("bandwidth")) / 125000, 2),
        "latency_ms": round(_num(ping.get("latency")), 2),
        "jitter_ms": round(_num(ping.get("jitter")), 2),
        # Ping while the line is busy downloading / uploading (Ookla's interquartile mean).
        # A big jump over idle ping is bufferbloat, which shows up as IPTV buffering.
        "download_latency_ms": _loaded_latency(result.get("download")),
        "upload_latency_ms": _loaded_latency(result.get("upload")),
        "packet_loss": round(_num(loss_raw), 1) if loss_raw is not None else None,
        "isp": result.get("isp") or "Unknown ISP",
        "server_name": server.get("name") or "Unknown Server",
        "server_location": server.get("location") or "Unknown Location",
        "server_country": server.get("country") or "Unknown Country",
        "server_host": server.get("host") or "Unknown host",
        "distance": f"{distance} km" if distance is not None else "N/A",
        "time_display": time_display,
        "time_local": time_local,
        # The public IP the test went out through: shows whether the VPN was in use.
        "exit_ip": exit_ip,
        # The result's page on speedtest.net (the CLI shares results by default).
        "result_url": result_url,
        # The network interface the test used (tun0 / wg0 through a VPN, eth0 without one),
        # and whether that's a VPN tunnel. None when the CLI didn't say.
        "interface_name": str(interface.get("name") or "").strip() or None,
        "via_vpn": _is_vpn_interface(interface),
    }
    summary["bufferbloat_grade"], extra = _bufferbloat(summary)
    summary["bufferbloat_ms"] = round(extra, 1) if extra is not None else None
    return summary


def _note_exit_change(summary, logger):
    """Remember this test's exit IP; if it differs from the last test's, record the old one
    in summary["exit_ip_was"]. A new exit IP usually means Gluetun or the VPN picked another
    server, which is the first thing to know when the numbers suddenly change.

    The last exit IP is kept in Redis and in the plugin's data folder. The file matters: a
    new exit IP usually comes with a Gluetun restart, which often restarts Dispatcharr too,
    and Dispatcharr's Redis may start empty after that.
    """
    new = summary.get("exit_ip")
    if not new:
        return
    client = _get_redis_client(logger)
    old = None
    if client is not None:
        try:
            old = _s(client.get(_LAST_EXIT_IP_KEY))
            client.set(_LAST_EXIT_IP_KEY, new)
        except Exception:
            old = None
    old = old or _local_last_exit_ip.get("ip") or _read_last_exit_ip()
    _local_last_exit_ip["ip"] = new
    _write_last_exit_ip(new, logger)
    if old and old != new:
        summary["exit_ip_was"] = old


def _last_exit_ip_path():
    return os.path.join(PLUGIN_DATA_DIR, _LAST_EXIT_IP_FILE)


def _read_last_exit_ip():
    try:
        with open(_last_exit_ip_path(), encoding="utf-8") as fh:
            return fh.read().strip() or None
    except OSError:
        return None


def _write_last_exit_ip(ip, logger):
    path = _last_exit_ip_path()
    tmp = f"{path}.new.{os.getpid()}"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(ip)
        os.replace(tmp, path)
    except OSError:
        logger.debug("Packet Slapper: could not save the last exit IP", exc_info=True)


def _format_popup_message(s):
    """Short one-line summary for the Dispatcharr notification popup.

    The popup doesn't grow to fit, so it only carries what you need at a glance;
    the full details are in the Discord post and in the log line from
    _format_ui_message.
    """
    parts = [
        f"\u2b07 {s['download_mbps']} Mbps",
        f"\u2b06 {s['upload_mbps']} Mbps",
        f"Ping {s['latency_ms']} ms",
        f"Loss {_fmt_loss(s['packet_loss'])}",
        f"Jitter {s['jitter_ms']} ms",
    ]
    down, up = s.get("download_latency_ms"), s.get("upload_latency_ms")
    if down is not None and up is not None:
        parts.append(f"Loaded {down:.0f}/{up:.0f} ms")  # download/upload, rounded to fit the popup
    elif down is not None or up is not None:
        arrow, value = ("\u2b07", down) if down is not None else ("\u2b06", up)
        parts.append(f"Loaded {arrow} {value:.0f} ms")
    if s.get("bufferbloat_grade"):
        parts.append(f"Bloat {s['bufferbloat_grade']}")
    if s.get("server_location") and s["server_location"] != "Unknown Location":
        parts.append(f"Host: {s['server_location']}")
    if s.get("exit_ip"):
        parts.append(f"Source: {s['exit_ip']}")  # the public IP the test went out through
    if s.get("exit_ip_was"):
        parts.append(f"New exit IP (was {s['exit_ip_was']})")
    if s.get("buffering"):
        parts.append(f"Buffering {s['buffering'].split(' (')[0]}")  # count only: the popup is short
    return " | ".join(parts)


def _format_ui_message(s):
    """Full one-line summary, written to the Dispatcharr log for every run.

    Fields are separated with ' | ' and the numbers you care about come first.
    """
    parts = [
        f"\u2b07 {s['download_mbps']} Mbps",
        f"\u2b06 {s['upload_mbps']} Mbps",
        f"Ping {s['latency_ms']} ms (jitter {s['jitter_ms']} ms)",
    ]
    loaded = _fmt_loaded(s)
    if loaded:
        parts.append(f"Loaded ping {loaded}")
    if _fmt_bloat(s):
        parts.append(f"Bufferbloat {_fmt_bloat(s)}")
    parts += [
        f"Loss {_fmt_loss(s['packet_loss'])}",
        f"{s['server_name']}, {s['server_location']}",
        f"ISP: {s['isp']}",
    ]
    if s.get("exit_ip"):
        parts.append(f"Exit IP {s['exit_ip']}" + (f" (was {s['exit_ip_was']})" if s.get("exit_ip_was") else ""))
    if s["distance"] != "N/A":
        parts.append(s["distance"])
    if s.get("buffering"):
        parts.append(f"Buffering {s['buffering']}")
    parts.append(s["time_local"])
    if s.get("result_url"):
        parts.append(s["result_url"])
    return " | ".join(parts)


def _discord_style(settings):
    return "plain" if str(settings.get("discord_style") or "embed").lower() == "plain" else "embed"


def _format_discord_message(s, include_ip=False):
    """Classic plain-text Discord message."""
    lines = [
        "**Packet Slapper speedtest**",
        f"> Server: {s['server_name']} ({s['server_location']}, {s['server_country']})",
        f"> Host: {s['server_host']}",
        f"> ISP: {s['isp']}" + (f" (`{s['exit_ip']}`)" if include_ip and s.get("exit_ip") else "")
        + (f" \u2014 {_exit_change_text(s, include_ip)}" if s.get("exit_ip_was") else ""),
        f"> Time: {s['time_display']}",
        f"> Latency: `{s['latency_ms']} ms` (Jitter: {s['jitter_ms']} ms)"
        + (f"\n> Loaded latency: {_fmt_loaded(s)}" if _fmt_loaded(s) else "")
        + (f"\n> Bufferbloat: {_fmt_bloat(s)}" if _fmt_bloat(s) else ""),
        f"> Download: `{s['download_mbps']} Mbps`",
        f"> Upload: `{s['upload_mbps']} Mbps`",
        f"> Packet Loss: `{_fmt_loss(s['packet_loss'], ' ')}`",
    ]
    if s["distance"] != "N/A":
        lines.append(f"> Distance: {s['distance']}")
    if s.get("buffering"):
        lines.append(f"> Buffering: {s['buffering']}")
    if s.get("result_url"):
        lines.append(f"> Result: <{s['result_url']}>")  # <...> stops Discord adding a preview card
    return "\n".join(lines)


def _loaded_short(s):
    """'⬇ 108 · ⬆ 17 ms' (whole ms, for a narrow field), or None."""
    down, up = s.get("download_latency_ms"), s.get("upload_latency_ms")
    parts = []
    if down is not None:
        parts.append(f"\u2b07 {down:.0f}")
    if up is not None:
        parts.append(f"\u2b06 {up:.0f}")
    return " \u00b7 ".join(parts) + " ms" if parts else None


def _discord_embed(s, include_ip=False):
    """Discord embed. Every inline field holds one short value so the rows line up
    (speeds and ping, then jitter, loss and loaded ping); server, ISP and the result
    link get full-width lines below, where longer text has room."""
    fields = [
        {"name": "\u2b07 Download", "value": f"**{s['download_mbps']}** Mbps", "inline": True},
        {"name": "\u2b06 Upload", "value": f"**{s['upload_mbps']}** Mbps", "inline": True},
        {"name": "\u23f1 Ping", "value": f"**{s['latency_ms']}** ms", "inline": True},
        {"name": "\u3030 Jitter", "value": f"{s['jitter_ms']} ms", "inline": True},
        {"name": "\U0001f4e6 Packet loss", "value": _fmt_loss(s['packet_loss'], ' '), "inline": True},
    ]
    loaded = _loaded_short(s)
    if loaded:
        fields.append({"name": "\u23f3 Loaded ping", "value": loaded, "inline": True})
    if s.get("bufferbloat_grade"):
        fields.append({"name": "\U0001f6a6 Bufferbloat",
                       "value": f"**{s['bufferbloat_grade']}** \u00b7 ping rises {s['bufferbloat_ms']:.0f} ms under load",
                       "inline": False})

    where = f"{s['server_location']}, {s['server_country']}"
    server = f"{_clip(s['server_name'], 100)} \u00b7 {_clip(where, 120)}"
    if s["distance"] != "N/A":
        server += f" \u00b7 {s['distance']}"
    if s.get("server_host") and s["server_host"] != "Unknown host":
        server += f"\n{_clip(s['server_host'], 120)}"  # plain text, so it wraps instead of breaking mid-word
    fields.append({"name": "\U0001f4cd Server", "value": server, "inline": False})

    isp = _clip(s["isp"], 200)
    if include_ip and s.get("exit_ip"):
        isp += f" \u00b7 `{_clip(s['exit_ip'], 60)}`"
    if s.get("exit_ip_was"):
        isp += f"\n\U0001f504 {_exit_change_text(s, include_ip)}"
    fields.append({"name": "\U0001f310 ISP", "value": isp, "inline": False})
    if s.get("buffering"):
        fields.append({"name": "\U0001f4fa Buffering", "value": _clip(s["buffering"], 300), "inline": False})

    embed = {
        "title": "Packet Slapper speedtest",
        "color": 0x2ECC71,
        "fields": fields,
        "footer": {"text": _clip(s["time_display"], 200)},
    }
    if s.get("result_url"):
        embed["url"] = s["result_url"]  # the title links to the result page too
        fields.append({"name": "\U0001f517 Result", "value": f"[View on speedtest.net]({s['result_url']})", "inline": False})
    return {"embeds": [embed]}


def _exit_change_text(s, include_ip):
    """'New exit IP since the last test' (with the old IP only if IPs may be posted)."""
    if include_ip:
        return f"new exit IP since the last test (was `{_clip(s['exit_ip_was'], 60)}`)"
    return "new exit IP since the last test"


def _discord_include_ip(settings):
    return bool(settings.get("discord_include_exit_ip", False))


def _discord_result_payload(s, style, include_ip=False):
    return _format_discord_message(s, include_ip) if style == "plain" else _discord_embed(s, include_ip)


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
            "User-Agent": _user_agent(),  # Discord's CDN rejects the default Python UA
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15):  # noqa: S310 - user-supplied webhook, by design
            pass
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
    """Dispatcharr's shared Redis client, or None if it isn't available."""
    try:
        from core.utils import RedisClient  # type: ignore
        return RedisClient.get_client()
    except Exception:
        logger.debug("Packet Slapper: Dispatcharr's Redis client is unavailable", exc_info=True)
        return None


# The Redis keys Dispatcharr itself uses to track who is connected (the same ones its
# stream-limit check reads in apps/proxy/utils.py), so a viewer counts exactly as long as
# Dispatcharr considers them connected.
# Live TV and catch-up: one set per channel holding the IDs of its connected clients.
# ts_proxy is the module's name in older Dispatcharr versions, live_proxy in newer ones.
_LIVE_PATTERNS = ("live:channel:*:clients", "ts_proxy:channel:*:clients")
_CATCHUP_PATTERNS = ("timeshift:channel:*:clients",)
# VOD: one hash per playback session. It stays around for a while after playback stops
# (so a seek or quick resume can reuse it), so only its active_streams count says whether
# anyone is actually watching. (vod_session_user:* is only a login mapping that expires
# 5 minutes after a session starts, whether or not it's still playing.)
_VOD_PATTERNS = ("vod_persistent_connection:*",)
# DVR recordings in progress, or starting within this many seconds: a speedtest takes up to
# a minute, so one that starts just before a recording could disrupt its first moments.
_RECORDING_LEAD_SECONDS = 180
_RECORDING_DONE = {"completed", "interrupted", "cancelled", "stopped", "failed"}
# Open connections per provider profile. Covers every session type, so it acts as a
# catch-all if a future Dispatcharr version moves the keys above.
_PROVIDER_COUNTER_PATTERNS = ("profile_connections:*",)


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


def _vod_active_streams(client, key):
    """How many streams are actually playing in a VOD session hash (0 when it's idle)."""
    if _s(client.type(key)) != "hash":
        return 0
    try:
        return int(_s(client.hget(key, "active_streams")) or 0)
    except (TypeError, ValueError):
        return 0


def _counter_value(client, key):
    try:
        return int(_s(client.get(key)) or 0)
    except (TypeError, ValueError):
        return 0


def _active_recordings(logger, now=None):
    """IDs of DVR recordings in progress or about to start, from Dispatcharr's schedule.

    A recording also shows up as a live viewer (it streams through Dispatcharr's proxy),
    but only once it has started; this also covers the few minutes before it begins.
    Returns [] if the schedule can't be read.
    """
    try:
        from datetime import timedelta

        from apps.channels.models import Recording  # type: ignore
        from django.utils import timezone as dj_timezone  # type: ignore
    except Exception:
        return []
    try:
        current = now or dj_timezone.now()
        rows = Recording.objects.filter(
            start_time__lte=current + timedelta(seconds=_RECORDING_LEAD_SECONDS),
            end_time__gte=current,
        ).values("id", "custom_properties")
        return [
            row["id"] for row in rows
            if str((row.get("custom_properties") or {}).get("status") or "").lower() not in _RECORDING_DONE
        ]
    except Exception:
        logger.debug("Packet Slapper: could not read the DVR schedule", exc_info=True)
        return []
    finally:
        try:
            from django.db import close_old_connections
            close_old_connections()
        except Exception:
            pass


def _recent_buffering(logger, window=_BUFFERING_WINDOW, now=None):
    """Buffering Dispatcharr logged in the last `window` seconds, from its own event log
    (the 'channel_buffering' system events): (total, [(channel, count), ...] most first),
    or None if the log can't be read.

    Dispatcharr keeps a limited number of recent events (its "max system events" setting),
    so a busy server may only have the last part of the hour; the counts are a floor.
    """
    try:
        from datetime import timedelta

        from core.models import SystemEvent  # type: ignore
        from django.utils import timezone as dj_timezone  # type: ignore
    except Exception:
        return None
    try:
        since = (now or dj_timezone.now()) - timedelta(seconds=window)
        names = list(
            SystemEvent.objects.filter(event_type="channel_buffering", timestamp__gte=since)
            .values_list("channel_name", flat=True)[:_BUFFERING_MAX_EVENTS]
        )
    except Exception:
        logger.debug("Packet Slapper: could not read Dispatcharr's buffering events", exc_info=True)
        return None
    finally:
        try:
            from django.db import close_old_connections
            close_old_connections()
        except Exception:
            pass
    counts = {}
    for name in names:
        key = str(name or "").strip() or "Unknown channel"
        counts[key] = counts.get(key, 0) + 1
    return len(names), sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))


def _fmt_buffering(total, channels, limit=3):
    """'3× in the last hour (ESPN 2, CNN 1)'."""
    top = ", ".join(f"{_clip(name, 30)} {count}" for name, count in channels[:limit])
    more = f", +{len(channels) - limit} more" if len(channels) > limit else ""
    return f"{total}\u00d7 in the last hour ({top}{more})"


def _check_buffering_alert(settings, logger, client):
    """If Dispatcharr logged at least 'Alert: buffering per hour' buffering events in the
    last hour, log it and post to Discord with the latest speedtest, at most once an hour.
    Returns True if an alert went out."""
    limit = _threshold(settings, "alert_buffering_per_hour", logger)
    if limit is None:
        return False
    buffering = _recent_buffering(logger)
    if not buffering or buffering[0] < limit:
        return False
    now = time.time()
    if client is not None:
        try:
            if not client.set(_BUFFERING_ALERT_KEY, str(now), nx=True, ex=_BUFFERING_WINDOW):
                return False  # already alerted within the hour
        except Exception:
            return False
    total, channels = buffering
    what = _fmt_buffering(total, channels, limit=5)
    last = _load_last_run(logger) or {}
    if last.get("at"):
        age = now - last["at"]
        test = (f"Last speedtest {_short_time(last['at'], settings)} ({_fmt_duration(age)} ago): "
                f"{last.get('detail')}")
        if age > _BUFFERING_WINDOW:
            test += ". That's over an hour old; Run speedtest now checks the connection as it is now."
    else:
        test = "No speedtest yet; Run speedtest now checks the connection."
    logger.warning(f"Packet Slapper: buffering {what} | {test}")
    webhook_url = str(settings.get("discord_webhook_url") or "").strip()
    if _discord_style(settings) == "plain":
        payload = f"\u26a0 **Buffering in Dispatcharr:** {what}\n{test}"
    else:
        payload = {"embeds": [{
            "title": "\u26a0 Buffering in Dispatcharr",
            "description": _clip(f"{what}\n\n{test}", 1500),
            "color": 0xF39C12,
        }]}
    _post_to_discord(webhook_url, payload, logger)
    return True


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
            "vod": collect(_VOD_PATTERNS, lambda k: _vod_active_streams(client, k)),
            "catch-up": collect(_CATCHUP_PATTERNS, lambda k: _key_size(client, k)),
            "provider connections": collect(_PROVIDER_COUNTER_PATTERNS, lambda k: _counter_value(client, k)),
            "recording": {f"recording:{rid}": 1 for rid in _active_recordings(logger)},
        }
    except Exception:
        logger.exception("Packet Slapper: failed reading viewer activity from Redis")
        return None


def _viewer_count(activity):
    """Live + VOD + catch-up sessions in a _detect_activity() result (provider counters and
    recordings excluded: a recording also counts as a live session while it runs)."""
    if not activity:
        return 0
    return sum(sum(keys.values()) for category, keys in activity.items()
               if category not in ("provider connections", "recording"))


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
    """Debug aid: log Redis keys that could relate to playback, with type and TTL.

    Only runs with debug logging on: it's six extra keyspace scans and up to 60 log lines.
    """
    try:
        if not logger.isEnabledFor(logging.DEBUG):
            return
    except Exception:
        return
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
                    logger.debug(f"Packet Slapper: [debug] more than {limit} related keys, stopping list")
                    return
                logger.debug(
                    f"Packet Slapper: [debug] redis key {key} "
                    f"type={_s(client.type(key))} ttl={client.ttl(key)}"
                )
        if not seen:
            logger.debug("Packet Slapper: [debug] no playback-related Redis keys found")
    except Exception:
        logger.exception("Packet Slapper: failed listing related Redis keys")


_CATEGORY_LABELS = {"live": "live", "vod": "VOD", "catch-up": "catch-up"}
# Marks "someone was watching" notes. Not ⚠, which only ever means an alert. The
# trailing U+FE0E asks for the plain text symbol rather than an emoji where fonts allow.
_PLAYING = "\u25b6\ufe0e"
_RECORDING = "\u25cf"  # ● marks DVR recordings


def _describe_active_streams(logger):
    """Result text for the 'Check active streams' button.

    Short enough for the popup; the Redis keys that matched go to the log.
    """
    activity = _detect_activity(logger)
    if activity is None:
        return "Could not read viewer info from Redis; scheduled runs will treat this as idle. See the Dispatcharr log."

    busy = {category: keys for category, keys in activity.items() if keys}
    if busy:
        logger.info(f"Packet Slapper: Check Active Streams matched: {busy}")
    else:
        logger.info("Packet Slapper: Check Active Streams found no active sessions")
    _log_related_keys(logger)

    if not busy:
        return "\u2713 No active sessions | Scheduled tests would run"
    n = _viewer_count(activity)
    parts = []
    if n:
        kinds = [
            f"{sum(busy[c].values())} {label}"
            for c, label in _CATEGORY_LABELS.items() if c in busy
        ]
        parts.append(f"{_PLAYING} {n} active session{'s' if n != 1 else ''} ({', '.join(kinds)})")
    recordings = len(busy.get("recording") or {})
    if recordings:
        parts.append(f"{_RECORDING} {recordings} recording{'s' if recordings != 1 else ''} now or starting soon")
    if not parts:
        parts.append(f"{_PLAYING} Provider connections in use")
    return " | ".join(parts) + " | Scheduled tests would be skipped"


# ---------------------------------------------------------------------------
# Alerts: VPN leak and thresholds
# ---------------------------------------------------------------------------

def _threshold(settings, key, logger):
    """A positive number from an alert setting, or None when it's blank, zero or not a number."""
    raw = settings.get(key)
    if raw in (None, ""):
        return None
    try:
        value = float(str(raw).strip())
    except ValueError:
        logger.warning(f"Packet Slapper: ignoring alert setting {key}={raw!r} (not a number)")
        return None
    return value if value > 0 else None


def _vpn_check_mode(settings):
    mode = str(settings.get("vpn_check") or "auto").strip().lower()
    return mode if mode in ("auto", "always", "off") else "auto"


def _vpn_seen(logger):
    """What VPN check 'Auto' remembers: {'interface': 'tun0', 'at': ...} once a test used a tunnel."""
    client = _get_redis_client(logger)
    if client is not None:
        seen = _redis_get_json(client, _VPN_SEEN_KEY)
        if isinstance(seen, dict):
            return seen
    return dict(_local_vpn_seen) or None


def _set_vpn_seen(logger, seen):
    _local_vpn_seen.clear()
    if seen:
        _local_vpn_seen.update(seen)
    client = _get_redis_client(logger)
    if client is None:
        return
    try:
        if seen:
            client.set(_VPN_SEEN_KEY, json.dumps(seen))  # no TTL: remembered across restarts
        else:
            client.delete(_VPN_SEEN_KEY)
    except Exception:
        logger.debug("Packet Slapper: could not save the VPN check state", exc_info=True)


def _check_vpn(summary, settings, logger):
    """Return (short, detail) if this test should have gone through a VPN and didn't, else None.

    Auto: once any test has gone through a VPN tunnel, every later test is expected to.
    Always: every test is expected to. Off: never checked (and Auto's memory is cleared).
    A test that doesn't say which interface it used is never an alert.
    """
    mode = _vpn_check_mode(settings)
    via = summary.get("via_vpn")
    if mode == "off":
        if _vpn_seen(logger):
            _set_vpn_seen(logger, None)
        return None
    if via is True:
        _set_vpn_seen(logger, {"interface": summary.get("interface_name"), "at": time.time()})
        return None
    if via is None:
        return None
    seen = _vpn_seen(logger)
    if mode == "auto" and not seen:
        return None  # no VPN so far: nothing to compare against
    used = summary.get("interface_name") or "a regular network connection"
    before = f" Earlier tests went through {seen.get('interface') or 'a VPN tunnel'}." if seen else ""
    return ("VPN off", f"VPN not in use: this test went out through {used}, not a VPN tunnel.{before}")


def _evaluate_alerts(summary, settings, logger):
    """Return {code: (short, detail)} for every alert this result trips, in a fixed order."""
    alerts = {}
    vpn = _check_vpn(summary, settings, logger)
    if vpn:
        alerts["vpn"] = vpn

    def low(code, key, value, short, label, unit):
        limit = _threshold(settings, key, logger)
        if limit is not None and value is not None and value < limit:
            alerts[code] = (f"{short}<{limit:g}", f"{label} {value:g} {unit} is below your {limit:g} {unit} alert.")

    def high(code, key, value, short, label, unit, extra=""):
        limit = _threshold(settings, key, logger)
        if limit is not None and value is not None and value > limit:
            alerts[code] = (f"{short}>{limit:g}{unit}",
                            f"{label} {value:g}{'' if unit == '%' else ' '}{unit} is above your {limit:g}{'' if unit == '%' else ' '}{unit} alert{extra}.")

    low("download", "alert_min_download_mbps", summary.get("download_mbps"), "\u2b07", "Download", "Mbps")
    low("upload", "alert_min_upload_mbps", summary.get("upload_mbps"), "\u2b06", "Upload", "Mbps")
    high("loss", "alert_max_loss_pct", summary.get("packet_loss"), "Loss", "Packet loss", "%")
    high("loaded", "alert_max_loaded_latency_ms", summary.get("download_latency_ms"), "Loaded", "Loaded ping \u2b07", "ms",
         " (bufferbloat, a common cause of buffering)")
    worst = str(settings.get("alert_bufferbloat_worse_than") or "off").strip().upper()
    grade = summary.get("bufferbloat_grade")
    if worst in _GRADE_ORDER and grade in _GRADE_ORDER and _GRADE_ORDER.index(grade) > _GRADE_ORDER.index(worst):
        alerts["bloat"] = (f"Bloat {grade}",
                           f"Bufferbloat grade {grade} (+{summary['bufferbloat_ms']:.0f} ms under load) is worse than "
                           f"your {worst} alert. Smart Queue / SQM on the router usually fixes this.")
    return alerts


def _load_alert_state(logger):
    client = _get_redis_client(logger)
    if client is not None:
        state = _redis_get_json(client, _ALERT_STATE_KEY)
        if isinstance(state, dict):
            return state
    return dict(_local_alert_state)


def _save_alert_state(logger, alerts, previous):
    """Remember which alerts are active. 'since' is kept while the same alerts continue."""
    if alerts:
        since = previous.get("since") if previous.get("alerts") else None
        state = {"alerts": {code: list(v) for code, v in alerts.items()}, "since": since or time.time()}
    else:
        state = {}
    _local_alert_state.clear()
    _local_alert_state.update(state)
    client = _get_redis_client(logger)
    if client is not None:
        try:
            if state:
                client.set(_ALERT_STATE_KEY, json.dumps(state))
            else:
                client.delete(_ALERT_STATE_KEY)
        except Exception:
            logger.debug("Packet Slapper: could not save the alert state", exc_info=True)


def _discord_post_mode(settings):
    return "alerts" if str(settings.get("discord_post_mode") or "every").lower() == "alerts" else "every"


def _alert_payload(payload, alerts, cleared, style):
    """Mark a result post as an alert (or a recovery). Returns the payload unchanged when neither."""
    if not alerts and not cleared:
        return payload
    lines = []
    if alerts:
        lines += [f"\u26a0 {detail}" for _, detail in alerts.values()]
    if cleared:
        names = ", ".join(short for short, _ in cleared.values())
        lines.append(f"\u2705 Back to normal ({names})" if not alerts else f"\u2705 Cleared: {names}")
    if style == "plain":
        return "\n".join(lines) + "\n" + payload
    embed = dict(payload["embeds"][0])
    if alerts:
        embed["title"] = "\u26a0 Packet Slapper speedtest"
        embed["color"] = 0xE74C3C if "vpn" in alerts else 0xF39C12  # red for a leak, amber otherwise
    else:
        embed["title"] = "\u2705 Packet Slapper speedtest"
    embed["description"] = _clip("\n".join(lines), 1500)
    return {"embeds": [embed]}


def _alert_popup_suffix(alerts, cleared):
    if alerts:
        return " | \u26a0 " + ", ".join(short for short, _ in alerts.values())
    if cleared:
        return " | \u2705 Back to normal"
    return ""


def _execute_run(settings, logger, manual=False, progress=None):
    progress = progress or (lambda percent, message=None: None)
    prometheus = bool(settings.get("prometheus_enabled", False))
    if not manual and settings.get("skip_if_streaming", True) and _any_active_streams(logger):
        logger.info("Packet Slapper: skipping scheduled run, active streams detected")
        if prometheus:
            _publish_skip(logger)
        return {"status": "skipped", "message": "Active streams detected; run skipped"}

    watching_note = ""
    if manual:
        # Run Now isn't skipped, but the user should know a viewer may have noticed, and
        # that the numbers were measured while sharing the connection with them.
        try:
            activity = _detect_activity(logger)
        except Exception:
            activity = None
        if activity and any(activity.values()):
            n = _viewer_count(activity)
            who = f"{n} active session{'s' if n != 1 else ''}" if n else "active provider connections"
            watching_note = f" | {_PLAYING} {who}"

    tz_name = str(settings.get("display_timezone") or _DEFAULT_TIMEZONE).strip()
    webhook_url = str(settings.get("discord_webhook_url") or "").strip()
    style = _discord_style(settings)

    try:
        server_id = _clean_server_id(settings.get("server_id"))
        progress(15, "Testing ping, download and upload (about 30 seconds)")
        result = _run_speedtest(logger, server_id, _auto_update(settings))
        summary = _summarize_result(result, tz_name)
        _note_exit_change(summary, logger)
    except Exception as exc:
        logger.error(f"Packet Slapper: speedtest failed: {exc}", exc_info=not isinstance(exc, RuntimeError))
        previous = _load_alert_state(logger)
        previous_alerts = {c: tuple(v) for c, v in (previous.get("alerts") or {}).items()}
        # A failed test can't tell whether the other alerts still apply, so they're kept as they were.
        alerts = dict(previous_alerts)
        alerts["failed"] = ("Test failed", f"Speedtest failed: {_clip(str(exc), 300)}")
        if _discord_post_mode(settings) == "every" or "failed" not in previous_alerts:
            _post_to_discord(webhook_url, _discord_error_payload(str(exc), style), logger)
        _save_alert_state(logger, alerts, previous)
        if prometheus:
            _publish_metrics(logger, ok=False)
        return {"status": "error", "message": f"\u274c Speedtest failed: {exc}"}

    progress(90, "Checking alerts and sending results")
    buffering = _recent_buffering(logger)
    if buffering and buffering[0]:
        summary["buffering"] = _fmt_buffering(*buffering)
    log_line = _format_ui_message(summary)
    if watching_note:
        log_line += f"{watching_note} during this test, so results may be low and playback may have stuttered"
    logger.info(f"Packet Slapper: {log_line}")  # every run, scheduled or not, lands in the log
    previous = _load_alert_state(logger)
    previous_alerts = {c: tuple(v) for c, v in (previous.get("alerts") or {}).items()}
    alerts = _evaluate_alerts(summary, settings, logger)
    cleared = {c: v for c, v in previous_alerts.items() if c not in alerts}
    for _, detail in alerts.values():
        logger.warning(f"Packet Slapper: alert: {detail}")
    if cleared and not alerts:
        logger.info("Packet Slapper: all alerts cleared")
    ui_message = _format_popup_message(summary) + _alert_popup_suffix(alerts, cleared) + watching_note
    alert_names = [name for name, _ in alerts.values()]

    if _discord_post_mode(settings) == "every":
        post = True
    else:
        # Alerts only: once when a problem starts or changes, once when it clears.
        post = (bool(alerts) and set(alerts) != set(previous_alerts)) or (not alerts and bool(previous_alerts))
    posted = None
    if post:
        payload = _discord_result_payload(summary, style, _discord_include_ip(settings))
        posted = _post_to_discord(webhook_url, _alert_payload(payload, alerts, cleared, style), logger)
    _save_alert_state(logger, alerts, previous)
    if posted is True:
        ui_message += " | Discord \u2713"
    elif posted is False:
        ui_message += " | Discord \u2717 (see log)"

    if prometheus and not _publish_metrics(logger, ok=True, summary=summary):
        # The test itself worked, so this isn't retried early: that would spend bandwidth on a
        # new test every minute for as long as Redis is down.
        return {"status": "publish_error", "message": ui_message + " | Prometheus \u2717 (see log)",
                "result": summary, "alerts": alert_names}

    # "message" is what Dispatcharr shows in the popup; "result" carries the raw numbers.
    return {"status": "ok", "message": ui_message, "result": summary, "alerts": alert_names}


# Compare-and-act scripts, so a lease is only renewed or removed by the holder that set it,
# even if it expired and someone else took it in between the check and the action.
_REFRESH_IF_OWNER = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"
)
_DELETE_IF_OWNER = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)


def _refresh_if_owner(client, key, owner, ttl):
    """Renew `key`'s TTL if `owner` still holds it. True if renewed."""
    try:
        return bool(client.eval(_REFRESH_IF_OWNER, 1, key, owner, int(ttl)))
    except Exception:
        pass
    try:  # Redis without EVAL (rare): non-atomic fallback
        if _s(client.get(key)) == owner:
            client.expire(key, int(ttl))
            return True
    except Exception:
        pass
    return False


def _delete_if_owner(client, key, owner):
    """Delete `key` if `owner` still holds it. True if deleted."""
    try:
        return bool(client.eval(_DELETE_IF_OWNER, 1, key, owner))
    except Exception:
        pass
    try:
        if _s(client.get(key)) == owner:
            client.delete(key)
            return True
    except Exception:
        pass
    return False


class _LeaseKeeper:
    """While a speedtest runs, renew the 'a test is running' slot, so a slow CLI download
    plus a slow test can't outlive it and let a second test start in another process."""

    def __init__(self, client, leader_id=None):
        self.client = client
        self.leader_id = leader_id
        self.owner = _instance_id()
        self._stop = threading.Event()
        self._thread = None

    def _renew(self):
        if _refresh_if_owner(self.client, _RUNNING_KEY, self.owner, _RUN_TTL):
            try:
                self.client.expire(_CURRENT_RUN_KEY, _RUN_TTL)
            except Exception:
                pass
        if self.leader_id and _refresh_if_owner(self.client, _LEADER_KEY, self.leader_id, _LEADER_TTL):
            try:
                self.client.expire(_STATE_KEY, _STATE_TTL)  # keep Status showing "test in progress"
            except Exception:
                pass

    def _loop(self):
        while not self._stop.wait(_KEEPALIVE_SECONDS):
            self._renew()

    def __enter__(self):
        if self.client is not None:
            self._thread = threading.Thread(target=self._loop, daemon=True, name="packet-slapper-keepalive")
            self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return False


def _begin_run(logger, manual=False):
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
    current = {"manual": bool(manual), "at": time.time()}
    _local_current_run.clear()
    _local_current_run.update(current)
    _redis_set(client, _CURRENT_RUN_KEY, json.dumps(current), _RUN_TTL)
    return True, client


def _end_run(client):
    _local_current_run.clear()
    if client is not None:
        try:
            client.delete(_CURRENT_RUN_KEY)
        except Exception:
            pass
        _delete_if_owner(client, _RUNNING_KEY, _instance_id())
    _run_lock.release()


def _do_one_run(settings, logger, manual=False, progress=None, leader_id=None):
    """Run (or skip) one speedtest and remember the outcome for Status.

    Blocks for the length of the speedtest (up to SPEEDTEST_TIMEOUT seconds). Called by the
    scheduler service, and by Run speedtest now, which Dispatcharr runs as a background task.
    progress: Dispatcharr's report_progress(percent, message) for Run speedtest now, or None.
    leader_id: for a scheduled run, the scheduler lock to keep renewed while it runs.
    """
    claimed, client = _begin_run(logger, manual)
    if not claimed:
        logger.info("Packet Slapper: a speedtest is already running; not starting another")
        return dict(_BUSY)
    try:
        with _LeaseKeeper(client, leader_id):
            outcome = _execute_run(settings, logger, manual, progress)
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


_BUSY = {"status": "busy", "message": "A speedtest is already running. Try again in a minute."}


def _task_result(outcome):
    """Turn a run's outcome into what Dispatcharr's task popup and run history expect.

    Dispatcharr knows three outcomes: ok (green), partial (yellow) and error (red). A test
    that tripped an alert, couldn't update Prometheus, or found another test running is
    partial; the original outcome stays in "outcome" for the run history's JSON download.
    """
    result = dict(outcome)
    status = outcome.get("status")
    result["outcome"] = status
    if status == "error":
        result["status"] = "error"
    elif status in ("busy", "publish_error") or outcome.get("alerts"):
        result["status"] = "partial"
    else:
        result["status"] = "ok"
    return result


def _run_now(settings, logger, progress, manual=True):
    """Run speedtest now. Dispatcharr runs this as a background task (the action is async),
    shows progress while it runs, and pops up the message when it's done.

    The scheduler queues the same action for scheduled tests (manual=False), so they show
    up in the plugin's run history too, marked as scheduled.
    """
    progress(5, "Starting the speedtest")
    outcome = _do_one_run(settings, logger, manual=manual, progress=progress)
    progress(100, "Done")
    return _task_result(outcome)


def _load_current_run(logger):
    client = _get_redis_client(logger)
    if client is not None:
        current = _redis_get_json(client, _CURRENT_RUN_KEY)
        if isinstance(current, dict):
            return current
    return dict(_local_current_run) or None


def _record_last_run(logger, outcome, manual):
    """Remember the outcome for the Status button.

    Real results (ok / error) are the "last run". A skipped run doesn't replace them:
    it only extends a separate "skipped N times in a row" streak, which a real run resets.
    """
    status = outcome.get("status")
    client = _get_redis_client(logger)
    if status == "skipped":
        previous = _load_skip_streak(logger) or {}
        record = {"at": time.time(), "count": int(previous.get("count") or 0) + 1}
        _local_last_skip.clear()
        _local_last_skip.update(record)
        _redis_set(client, _LAST_SKIP_KEY, json.dumps(record))  # no TTL: survives a stop
        return
    if status in ("ok", "publish_error"):
        r = outcome.get("result") or {}
        detail = f"\u2b07 {r.get('download_mbps')} / \u2b06 {r.get('upload_mbps')} Mbps"
        if r.get("bufferbloat_grade"):
            detail += f" \u00b7 bloat {r['bufferbloat_grade']}"
        if status == "publish_error":
            detail += " (Prometheus metrics NOT updated)"
    else:
        detail = str(outcome.get("message", ""))[:120]
    record = {"at": time.time(), "status": status, "manual": bool(manual), "detail": detail}
    _local_last_run.clear()
    _local_last_run.update(record)
    _redis_set(client, _LAST_RUN_KEY, json.dumps(record))  # no TTL: survives a stop
    _local_last_skip.clear()  # a real run ends any skip streak
    if client is not None:
        try:
            client.delete(_LAST_SKIP_KEY)
        except Exception:
            logger.debug("Packet Slapper: could not clear the skip streak", exc_info=True)


def _load_skip_streak(logger):
    client = _get_redis_client(logger)
    if client is not None:
        try:
            raw = client.get(_LAST_SKIP_KEY)
            return json.loads(_s(raw)) if raw else None
        except Exception:
            pass
    return dict(_local_last_skip) or None


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


def _publish_metrics(logger, ok, summary=None):
    """Write this run to the key the Dispatcharr Exporter reads. Returns True once Redis confirms it.

    A failed run only updates the attempt fields, so the last good numbers stay visible
    next to last_attempt_success=0. Retries a couple of times, since the scheduler thread
    is long-lived and its pooled connection can go stale.
    """
    for attempt in range(1, _METRICS_ATTEMPTS + 1):
        try:
            client = _get_redis_client(logger)  # fresh lookup each attempt
            if client is None:
                raise RuntimeError("Redis is unavailable")
            raw = client.get(_METRICS_KEY)  # not _redis_get_json: a failed read must not wipe the last result
            try:
                state = json.loads(_s(raw)) if raw else {}
            except ValueError:
                state = {}  # unreadable leftover: start over rather than fail every run
            if not isinstance(state, dict):
                state = {}
            now = time.time()
            state["last_attempt_timestamp_seconds"] = now
            state["last_attempt_success"] = 1 if ok else 0
            if ok and summary is not None:
                state["last_success_timestamp_seconds"] = now
                # The exit IP stays out of the metrics key: Prometheus data tends to travel
                # further (Grafana, shared dashboards) than the Dispatcharr UI does.
                state["result"] = {k: v for k, v in summary.items() if k not in ("exit_ip", "exit_ip_was", "buffering")}
            try:
                payload = json.dumps(state, allow_nan=False)
            except ValueError as exc:  # bad data, retrying can't fix it
                logger.warning(f"Packet Slapper: Prometheus metrics not published, result is not JSON-safe: {exc}")
                return False
            client.set(_METRICS_KEY, payload)
            if _redis_get_json(client, _METRICS_KEY) != state:
                raise RuntimeError("read-back did not match what was written")
            return True
        except Exception as exc:
            logger.warning(f"Packet Slapper: could not publish Prometheus metrics (attempt {attempt}/{_METRICS_ATTEMPTS}): {exc}")
            if attempt < _METRICS_ATTEMPTS:
                time.sleep(1)
    return False


def _publish_skip(logger):
    """Record a skipped scheduled run (someone was watching) in the Prometheus key, so alerts can
    tell "not tested because the box was busy" from "tests are failing". Best effort, one try."""
    try:
        client = _get_redis_client(logger)
        if client is None:
            return
        raw = client.get(_METRICS_KEY)
        try:
            state = json.loads(_s(raw)) if raw else {}
        except ValueError:
            state = {}
        if not isinstance(state, dict):
            state = {}
        state["last_skip_timestamp_seconds"] = time.time()
        client.set(_METRICS_KEY, json.dumps(state, allow_nan=False))
    except Exception as exc:
        logger.debug(f"Packet Slapper: could not record the skip for Prometheus: {exc}")


def _publish_state(client, state):
    state["heartbeat_at"] = time.time()
    _redis_set(client, _STATE_KEY, json.dumps(state), _STATE_TTL)


def _acquire_leadership(client, leader_id):
    """True if this scheduler holds (or just took) the scheduler lock."""
    if client is None:
        return True  # can't coordinate without Redis; assume a single process
    try:
        if client.set(_LEADER_KEY, leader_id, nx=True, ex=_LEADER_TTL):
            return True
    except Exception:
        return False
    return _refresh_if_owner(client, _LEADER_KEY, leader_id, _LEADER_TTL)


def _clear_schedule(client, sched, logger):
    """The scheduler was turned off in settings: forget the shared schedule."""
    sched.state.clear()
    if client is not None and _delete_if_owner(client, _LEADER_KEY, sched.leader_id):
        try:
            client.delete(_STATE_KEY)
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


_CATCH_UP_SECONDS = 3600  # a set time missed by less than this (e.g. a restart) still runs once


def _run_times(settings):
    """Parse 'Run at' into (sorted [(hour, minute)], [entries that couldn't be read])."""
    times, bad = set(), []
    for part in str(settings.get("run_at_times") or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        hh, _, mm = part.partition(":")
        try:
            h, m = int(hh), int(mm or 0)
        except ValueError:
            bad.append(part)
            continue
        if not (hh.strip().isdigit() and (not mm or mm.strip().isdigit()) and 0 <= h <= 23 and 0 <= m <= 59):
            bad.append(part)
            continue
        times.add((h, m))
    return sorted(times), bad


def _fmt_run_times(times):
    return ", ".join(f"{h:02d}:{m:02d}" for h, m in times)


def _schedule(settings):
    """('every', minutes), ('at', [(h, m), ...]) or None when the scheduler is off."""
    if str(settings.get("scheduler_interval") or "").strip().lower() == "times":
        times, _ = _run_times(settings)
        return ("at", times) if times else None
    minutes = _interval_minutes(settings)
    return ("every", minutes) if minutes is not None else None


def _tz(settings):
    name = str(settings.get("display_timezone") or _DEFAULT_TIMEZONE).strip()
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone.utc


def _slots(times, tz, now):
    """Epoch seconds of every listed time from yesterday to two days ahead, in order."""
    from datetime import timedelta
    today = datetime.fromtimestamp(now, tz).date()
    out = []
    for offset in (-1, 0, 1, 2):
        day = today + timedelta(days=offset)
        for h, m in times:
            out.append(datetime(day.year, day.month, day.day, h, m, tzinfo=tz).timestamp())
    return sorted(out)


def _next_slot(times, tz, now):
    return next(t for t in _slots(times, tz, now) if t > now)


def _prev_slot(times, tz, now):
    past = [t for t in _slots(times, tz, now) if t <= now]
    return past[-1] if past else None


def _test_running(client):
    """True if a speedtest is running in this process or (per Redis) in any other."""
    if _run_lock.locked():
        return True
    if client is not None:
        try:
            return bool(client.get(_RUNNING_KEY))
        except Exception:
            pass
    return False


def _scheduled_run(sched, client, settings, logger):
    """Start one scheduled test. Returns "skipped", "busy", "queued", or a finished run's status.

    The scheduler checks whether anyone is watching (or recording) and whether a test is
    already running, then hands the test to Dispatcharr's task queue as the 'Run speedtest
    now' action, marked scheduled. That puts every scheduled test in the plugin's run history
    next to the manual ones. If the queue can't be used, the test runs right here instead.
    """
    if settings.get("skip_if_streaming", True) and _any_active_streams(logger):
        logger.info("Packet Slapper: skipping scheduled run, active streams detected")
        if settings.get("prometheus_enabled", False):
            _publish_skip(logger)
        _record_last_run(logger, {"status": "skipped"}, manual=False)
        return "skipped"
    if _test_running(client):
        logger.info("Packet Slapper: a speedtest is already running; trying again shortly")
        return "busy"
    if sched.dispatch is not None:
        try:
            sched.dispatch("run_now", {"scheduled": True})
            return "queued"
        except Exception as exc:
            logger.warning(f"Packet Slapper: couldn't queue the scheduled test ({exc}); running it here instead")
    outcome = _do_one_run(settings, logger, manual=False, leader_id=sched.leader_id)
    return outcome.get("status") if isinstance(outcome, dict) else None


def _scheduler_tick_at(sched, client, settings, logger, times):
    """One pass of an 'At set times' schedule.

    slot_next is the next set time; it only moves on once that time has been handled, so a
    tick that lands a moment after it can't skip it. pending_at is an earlier one-off run:
    a catch-up after a restart, or a retry after a skip (every 30 minutes) or a busy slot
    (every minute), never later than the next set time.
    """
    state = sched.state
    tz = _tz(settings)
    now = time.time()
    label = f"{_fmt_run_times(times)}|{getattr(tz, 'key', 'UTC')}"
    state.pop("interval_minutes", None)
    state["run_at"] = _fmt_run_times(times)
    if state.get("at_label") != label or not state.get("slot_next"):
        state["slot_next"] = _next_slot(times, tz, now)  # times or timezone changed
        state["at_label"] = label
    slot_next = state["slot_next"]
    pending = state.get("pending_at")
    next_at = min(pending, slot_next) if pending else slot_next

    if next_at <= now:
        state["next_run_at"] = None  # shows "test in progress"
        _publish_state(client, state)
        run_status = None
        try:
            run_status = _scheduled_run(sched, client, settings, logger)
        except Exception:
            logger.exception("Packet Slapper: unhandled error in scheduled run")
        after = time.time()
        state["slot_next"] = _next_slot(times, tz, after)
        if run_status == "busy":
            state["pending_at"] = min(after + _BUSY_RETRY_SECONDS, state["slot_next"])
        elif run_status == "skipped":
            state["pending_at"] = min(after + _SKIP_RETRY_SECONDS, state["slot_next"])
        else:
            state.pop("pending_at", None)
        pending = state.get("pending_at")
        next_at = min(pending, state["slot_next"]) if pending else state["slot_next"]

    state["next_run_at"] = next_at
    _publish_state(client, state)
    return True


def _scheduler_tick(sched, client, settings, logger, holding):
    """One pass of the schedule.

    holding=False on the first pass after this process became the leader (or after the
    scheduler was turned back on): it picks up the schedule a previous leader left in Redis.
    """
    state = sched.state
    kind, value = _schedule(settings) or ("every", _DEFAULT_INTERVAL_MINUTES)

    if not holding:
        # Just became active: pick up the shared schedule if a previous leader left one.
        shared = _redis_get_json(client, _STATE_KEY) or {}
        state.clear()
        state.update({
            "started_at": shared.get("started_at") or time.time(),
            "next_run_at": shared.get("next_run_at"),
            "pid": os.getpid(),
        })
        for key in ("slot_next", "pending_at", "at_label"):
            if shared.get(key) is not None:
                state[key] = shared[key]
        if kind == "at" and not shared:
            # Fresh start: if a set time passed recently and nothing has run since, catch up once.
            now = time.time()
            prev = _prev_slot(value, _tz(settings), now)
            last = _load_last_run(logger) or {}
            if prev is not None and now - prev <= _CATCH_UP_SECONDS and (last.get("at") or 0) < prev:
                state["pending_at"] = now
        what = f"at {_fmt_run_times(value)}" if kind == "at" else f"every {value:g} min"
        logger.info(f"Packet Slapper: scheduler started ({what})")

    if kind == "at":
        return _scheduler_tick_at(sched, client, settings, logger, value)
    for key in ("slot_next", "pending_at", "at_label", "run_at"):
        state.pop(key, None)
    interval_seconds = value * 60
    state["interval_minutes"] = interval_seconds / 60
    now = time.time()
    next_at = state.get("next_run_at")
    if next_at and next_at - now > interval_seconds:  # interval was shortened
        next_at = state["next_run_at"] = now + interval_seconds

    if not next_at or next_at <= now:
        state["next_run_at"] = None  # shows "test in progress"
        _publish_state(client, state)
        run_status = None
        try:
            run_status = _scheduled_run(sched, client, settings, logger)
        except Exception:
            logger.exception("Packet Slapper: unhandled error in scheduled run")
        if run_status == "busy":
            delay = min(_BUSY_RETRY_SECONDS, interval_seconds)
        elif run_status == "skipped":
            delay = min(_SKIP_RETRY_SECONDS, interval_seconds)  # don't lose a whole interval to one evening of TV
        else:
            delay = interval_seconds
        state["next_run_at"] = time.time() + delay

    _publish_state(client, state)
    return True


class _Scheduler:
    """The schedule, run as this plugin's persistent service.

    Dispatcharr calls on_leader_acquired in one process and runs serve() on a thread it
    owns. It sets stop_event when the plugin is disabled, reloaded or updated, when
    Dispatcharr shuts down, or when this process loses leadership. Another process then
    becomes the leader and carries on from the schedule kept in Redis.
    """

    def __init__(self, get_settings, logger, dispatch=None):
        self.get_settings = get_settings
        self.logger = logger
        # Dispatcharr's dispatch_scheduled_task: queues a scheduled test as a background task.
        self.dispatch = dispatch
        self.state = {}
        self.running = False
        self.leader_id = f"{_instance_id()}-{uuid.uuid4().hex[:6]}"

    def _read_settings(self):
        try:
            return self.get_settings()
        except Exception:
            self.logger.debug("Packet Slapper: could not read the saved settings", exc_info=True)
            return None

    def serve(self, stop_event):
        logger = self.logger
        logger.info(f"Packet Slapper: scheduler service started (process {os.getpid()})")
        self.running = True
        client = None
        holding = False
        settings = None
        warned = False
        fail_since = None  # a failed settings read only warns if it keeps failing
        prewarmed = False
        standing_by = False
        last_buffering_check = 0.0
        stop_event.wait(_STARTUP_DELAY)
        try:
            while not stop_event.is_set():
                try:
                    if client is None:
                        client = _get_redis_client(logger)
                    fresh = self._read_settings()
                    if fresh is None:
                        fail_since = fail_since or time.time()
                        if not warned and time.time() - fail_since >= _DB_WARN_AFTER_SECONDS:
                            logger.warning(
                                "Packet Slapper: couldn't read this plugin's saved settings for over "
                                f"{_DB_WARN_AFTER_SECONDS} seconds (is the database reachable?); "
                                "carrying on with the last settings it read"
                            )
                            warned = True
                        if settings is None:
                            stop_event.wait(_TICK_SECONDS)  # nothing to go on yet
                            continue
                    else:
                        settings, fail_since, warned = fresh, None, False

                    if time.time() - last_buffering_check >= _BUFFERING_CHECK_SECONDS:
                        last_buffering_check = time.time()
                        _check_buffering_alert(settings, logger, client)

                    if not prewarmed:
                        prewarmed = True
                        _prewarm_cli(logger, settings)
                        if stop_event.is_set():
                            break

                    if _schedule(settings) is None:
                        if holding:
                            _clear_schedule(client, self, logger)
                            holding = False
                    elif _acquire_leadership(client, self.leader_id):
                        _scheduler_tick(self, client, settings, logger, holding)
                        holding = True
                    elif holding or not standing_by:
                        # Another scheduler holds the lock: leave the schedule to it.
                        self.state.clear()
                        holding, standing_by = False, True
                        logger.info(f"Packet Slapper: another process runs the schedule; "
                                    f"standing by (process {os.getpid()})")
                except Exception:
                    logger.exception("Packet Slapper: scheduler error")
                stop_event.wait(_TICK_SECONDS)
        finally:
            self.running = False
            # Hand the lock over but keep the schedule in Redis, so whoever runs it next
            # carries on from it instead of testing again at once. If nobody does, the state
            # key just expires.
            if holding and client is not None:
                _delete_if_owner(client, _LEADER_KEY, self.leader_id)
            logger.info(f"Packet Slapper: scheduler service stopped (process {os.getpid()})")


def _load_scheduler_state(logger):
    """Return (state or None, shared). shared=False means Redis was unavailable."""
    client = _get_redis_client(logger)
    if client is not None:
        try:
            raw = client.get(_STATE_KEY)
            return (json.loads(_s(raw)) if raw else None), True
        except Exception:
            logger.debug("Packet Slapper: could not read scheduler state from Redis", exc_info=True)
    sched = _active_scheduler
    alive = bool(sched is not None and sched.running and sched.state)
    return (dict(sched.state) if alive else None), False


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


_LAST_RUN_ICONS = {"ok": "\u2705", "skipped": "\u23ed", "error": "\u274c", "publish_error": "\u26a0"}


def _short_time(epoch, settings):
    """'14:19' for today, 'Nov 13 14:19' for another day, in the display timezone."""
    tz = _tz(settings)
    dt = datetime.fromtimestamp(epoch, tz)
    if dt.date() == datetime.now(tz).date():
        return dt.strftime("%H:%M")
    return f"{dt.strftime('%b')} {dt.day} {dt.strftime('%H:%M')}"


def _every_label(minutes):
    if minutes == 60:
        return "Every hour"
    if minutes > 60 and minutes % 60 == 0:
        return f"Every {minutes / 60:g} h"
    return f"Every {minutes:g} min"


def _describe_scheduler(settings, logger):
    """One short line for the Status popup (which doesn't grow to fit)."""
    enabled = _schedule(settings) is not None
    state, shared = _load_scheduler_state(logger)
    timed = str(settings.get("scheduler_interval") or "").strip().lower() == "times"
    now = time.time()

    parts = []
    if state and enabled:
        if state.get("run_at"):
            what = f"At {state['run_at']}"
        else:
            what = _every_label(state.get("interval_minutes") or _DEFAULT_INTERVAL_MINUTES)
        next_at = state.get("next_run_at")
        if not next_at:
            when = "test in progress"
        elif next_at <= now:
            when = "next run due now"
        else:
            when = f"next {_short_time(next_at, settings)} (in {_fmt_duration(next_at - now)})"
        parts.append(f"{what} \u00b7 {when}")
    elif state:
        parts.append("Scheduler stopping")
    elif enabled:
        parts.append("Scheduler starting")
    elif timed:
        parts.append("Scheduler off ('Run at' has no valid times)")
    else:
        parts.append("Scheduler off")
    if timed:
        _, bad = _run_times(settings)
        if bad:
            parts.append(f"Run at: couldn't read {', '.join(repr(b) for b in bad)} (use 15:00)")

    current = _load_current_run(logger)
    if current:
        kind = "Manual" if current.get("manual") else "Scheduled"
        parts.append(f"{kind} test in progress (started {_short_time(current['at'], settings)})")

    last = _load_last_run(logger)
    if last:
        kind = "manual" if last.get("manual") else "scheduled"
        icon = _LAST_RUN_ICONS.get(last.get("status"), "")
        parts.append(f"Last run {_short_time(last['at'], settings)} ({kind}) {icon} {last.get('detail')}".replace("  ", " "))
    else:
        parts.append("No runs yet")

    buffering = _recent_buffering(logger)
    if buffering is not None:
        parts.append(f"Buffering {_fmt_buffering(*buffering)}" if buffering[0] else "No buffering in the last hour")

    alert_state = _load_alert_state(logger)
    if alert_state.get("alerts"):
        names = ", ".join(v[0] for v in alert_state["alerts"].values())
        since = f" since {_short_time(alert_state['since'], settings)}" if alert_state.get("since") else ""
        parts.append(f"\u26a0 {names}{since}")

    skips = _load_skip_streak(logger)
    if skips and skips.get("count"):
        n = int(skips["count"])
        parts.append(f"Skipped {n}\u00d7 (someone watching)")

    if not shared:
        parts.append("(this worker only: Redis unavailable)")
    return " | ".join(parts)


def _manifest():
    """plugin.json is the single source of truth for name, version and description."""
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugin.json"), encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


_MANIFEST = _manifest()


_OLD_DISPATCHARR = (
    "Packet Slapper 2.x needs Dispatcharr 0.34.0 or newer. Update Dispatcharr, "
    "or install Packet Slapper 1.3.0, the last version for older Dispatcharr."
)


class Plugin:
    # Read from plugin.json so the two can't drift apart (the cause of the 1.0.3 fix).
    name = _MANIFEST.get("name", "Packet Slapper")
    version = _MANIFEST.get("version", "0.0.0")
    description = _MANIFEST.get("description", "")

    fields = []  # defined in plugin.json; kept here only if you drop the manifest

    # Nothing is started in __init__: Dispatcharr creates this class in every one of its
    # processes. The scheduler starts in on_leader_acquired, in the one process it picks.

    def get_fields(self, context):
        """Settings for Dispatcharr's page. The same as plugin.json, except that once
        'Find servers' has run, 'Speedtest server' becomes a dropdown of what it found.

        Dispatcharr calls this when it loads the plugin (at startup, and on the reload that
        Find servers asks for), so the choices follow the latest search.
        """
        fields = [dict(f) for f in (_MANIFEST.get("fields") or [])]
        try:
            servers = _load_server_list(context.get("data_dir"))
            if not servers:
                return fields
            try:
                saved = _clean_server_id((context.get("settings") or {}).get("server_id"))
            except RuntimeError:
                saved = ""
            return [_server_field(f, servers, saved) if f.get("id") == "server_id" else f for f in fields]
        except Exception:
            return fields  # never let a bad cache file hide the settings

    def on_leader_acquired(self, context):
        """Dispatcharr picked this process to run the schedule (needs persistent_service)."""
        global _active_scheduler
        logger = context.get("logger") or logging.getLogger("plugins.packet_slapper")
        _use_data_dir(context, logger)
        get_settings = context.get("get_settings") or (lambda: dict(context.get("settings") or {}))
        sched = _Scheduler(get_settings, logger, context.get("dispatch_scheduled_task"))
        _active_scheduler = sched
        context["start_service"](sched.serve, name="scheduler")

    def on_leader_lost(self, context):
        """Called after Dispatcharr has stopped the scheduler service in this process."""
        logger = context.get("logger") or logging.getLogger("plugins.packet_slapper")
        logger.info("Packet Slapper: this process no longer runs the schedule")

    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings", {}) or {}
        logger = context.get("logger") or logging.getLogger("plugins.packet_slapper")
        if "data_dir" not in context:
            # Dispatcharr 0.34 added data_dir along with the scheduler service and async
            # actions this version relies on. Without them the scheduler would never start.
            return {"status": "error", "message": _OLD_DISPATCHARR}
        _use_data_dir(context, logger)

        if action == "run_now":
            progress = context.get("report_progress") or (lambda percent, message=None: None)
            scheduled = bool((params or {}).get("scheduled"))  # queued by the scheduler
            return _run_now(settings, logger, progress, manual=not scheduled)

        if action == "scheduler_status":
            return {"status": "ok", "message": _describe_scheduler(settings, logger)}

        if action == "check_streams":
            return {"status": "ok", "message": _describe_active_streams(logger)}

        if action == "find_servers":
            return _describe_servers(settings, logger)

        if action == "test_webhook":
            webhook_url = settings.get("discord_webhook_url", "")
            posted = _post_to_discord(webhook_url, _discord_test_payload(_discord_style(settings)), logger)
            if posted is None:
                return {"status": "ok", "message": "No Discord webhook set. It's optional; results still show in Dispatcharr."}
            if posted:
                return {"status": "ok", "message": "Test message sent to Discord"}
            return {"status": "error", "message": "Discord post failed (check the webhook URL; see the Dispatcharr log)"}

        return {"status": "error", "message": f"Unknown action: {action}"}
