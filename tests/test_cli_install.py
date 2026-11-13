import logging
import os
import time

import pytest

from conftest import Env

log = logging.getLogger("t")


@pytest.fixture
def ps(tmp_path):
    e = Env(tmp_path)
    m = e.ps
    m._get_redis_client = lambda logger: None
    m.calls = {"probe": 0, "install": []}

    def probe(start, arch):
        m.calls["probe"] += 1
        return m.LATEST

    def install(version, arch, logger):
        m.calls["install"].append(version)
        os.makedirs(m.BIN_DIR, exist_ok=True)
        with open(m.SPEEDTEST_BIN, "w") as fh:
            fh.write("#!/bin/sh\n")
        os.chmod(m.SPEEDTEST_BIN, 0o700)
        with open(m.SPEEDTEST_VERSION_FILE, "w") as fh:
            fh.write(version)

    m.LATEST = "1.2.0"
    m._latest_cli_version = probe
    m._install_cli = install
    m._arch_suffix = lambda: "x86_64"
    return m


def installed(ps, version="1.2.0", checked_age=None):
    ps._install_cli(version, "x86_64", log)
    ps.calls["install"].clear()
    if checked_age is not None:
        ps._mark_update_checked()
        ps._cli_update_checked_at = 0.0  # simulate a fresh process: only the shared file counts
        t = time.time() - checked_age
        os.utime(ps.SPEEDTEST_CHECKED_FILE, (t, t))


def test_first_install_downloads_latest(ps):
    ps.LATEST = "1.2.3"
    ps._ensure_speedtest_binary(log)
    assert ps.calls == {"probe": 1, "install": ["1.2.3"]}
    assert os.path.exists(ps.SPEEDTEST_CHECKED_FILE)


def test_recent_check_is_not_repeated_by_another_worker(ps):
    installed(ps, checked_age=3600)
    ps._ensure_speedtest_binary(log)
    assert ps.calls["probe"] == 0


def test_check_is_repeated_after_a_day(ps):
    installed(ps, checked_age=25 * 3600)
    ps.LATEST = "1.3.0"
    ps._ensure_speedtest_binary(log)
    assert ps.calls == {"probe": 1, "install": ["1.3.0"]}


def test_auto_update_off_never_probes(ps):
    installed(ps, checked_age=30 * 24 * 3600)
    ps._ensure_speedtest_binary(log, auto_update=False)
    assert ps.calls == {"probe": 0, "install": []}


def test_auto_update_off_still_installs_a_missing_cli(ps):
    ps._ensure_speedtest_binary(log, auto_update=False)
    assert ps.calls == {"probe": 0, "install": [ps.SPEEDTEST_CLI_BASE_VERSION]}


def test_failed_check_keeps_the_installed_cli_and_backs_off(ps):
    installed(ps, checked_age=25 * 3600)

    def boom(start, arch):
        ps.calls["probe"] += 1
        raise OSError("network down")

    ps._latest_cli_version = boom
    assert ps._ensure_speedtest_binary(log) == ps.SPEEDTEST_BIN
    assert ps._ensure_speedtest_binary(log) == ps.SPEEDTEST_BIN
    assert ps.calls == {"probe": 1, "install": []}


def test_prewarm_only_runs_in_one_worker(tmp_path):
    from conftest import FakeRedis
    m = Env(tmp_path).ps
    r = FakeRedis()
    m._get_redis_client = lambda logger: r
    calls = []
    m._ensure_speedtest_binary = lambda logger, auto_update=True: calls.append(auto_update)
    m._real_prewarm_cli(log, {"cli_auto_update": False})
    m._PROCESS_TOKEN = "other"  # a second worker starting at the same time
    m._real_prewarm_cli(log, {})
    assert calls == [False]
