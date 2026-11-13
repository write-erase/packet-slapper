import fnmatch
import importlib.util
import logging
import os
import threading
import time

import pytest

PLUGIN_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugin.py")


class FakeRedis:
    """Just enough of redis-py for Packet Slapper, including the two EVAL scripts it uses."""

    def __init__(self):
        self.d, self.exp, self.lock = {}, {}, threading.Lock()

    def _gc(self):
        now = time.time()
        for k in [k for k, t in self.exp.items() if t <= now]:
            self.d.pop(k, None)
            self.exp.pop(k, None)

    def set(self, k, v, nx=False, ex=None):
        with self.lock:
            self._gc()
            if nx and k in self.d:
                return None
            self.d[k] = v if isinstance(v, str) else str(v)
            if ex:
                self.exp[k] = time.time() + ex
            else:
                self.exp.pop(k, None)
            return True

    def get(self, k):
        with self.lock:
            self._gc()
            return self.d.get(k)

    def delete(self, *ks):
        with self.lock:
            for k in ks:
                self.d.pop(k, None)
                self.exp.pop(k, None)

    def expire(self, k, ttl):
        with self.lock:
            self._gc()
            if k not in self.d:
                return False
            self.exp[k] = time.time() + int(ttl)
            return True

    def ttl(self, k):
        with self.lock:
            self._gc()
            if k not in self.d:
                return -2
            return int(self.exp[k] - time.time()) if k in self.exp else -1

    def eval(self, script, numkeys, key, owner, *args):
        with self.lock:
            self._gc()
            if self.d.get(key) != owner:
                return 0
            if "expire" in script:
                self.exp[key] = time.time() + int(args[0])
            else:
                self.d.pop(key, None)
                self.exp.pop(key, None)
            return 1

    def scan_iter(self, match="*", count=None):
        with self.lock:
            self._gc()
            return [k for k in list(self.d) if fnmatch.fnmatch(k, match)]

    def type(self, k):
        with self.lock:
            self._gc()
            v = self.d.get(k)
        if v is None:
            return "none"
        return {set: "set", dict: "hash"}.get(type(v), "string")

    def sadd(self, k, *members):
        with self.lock:
            self.d.setdefault(k, set()).update(members)

    def scard(self, k):
        v = self.get(k)
        return len(v) if isinstance(v, set) else 0

    def hset(self, k, field=None, value=None, mapping=None):
        with self.lock:
            h = self.d.setdefault(k, {})
            if mapping:
                h.update({f: str(v) for f, v in mapping.items()})
            if field is not None:
                h[field] = str(value)

    def hget(self, k, field):
        v = self.get(k)
        return v.get(field) if isinstance(v, dict) else None

    def hlen(self, k):
        v = self.get(k)
        return len(v) if isinstance(v, dict) else 0

    def llen(self, k):
        return 0

    zcard = llen


def load_plugin():
    spec = importlib.util.spec_from_file_location(f"packet_slapper_{time.time_ns()}", PLUGIN_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FAKE_RESULT = {
    "type": "result",
    "timestamp": "2026-11-13T11:00:00Z",
    "ping": {"latency": 9.12, "jitter": 0.71},
    "download": {"bandwidth": 114050000},
    "upload": {"bandwidth": 5225000},
    "packetLoss": 0,
    "isp": "Example ISP",
    "server": {"name": "Example", "location": "Chicago, IL", "country": "United States", "host": "speed.example.net", "distance": 12.3},
    "interface": {"internalIp": "172.18.0.5", "name": "eth0", "isVpn": False, "externalIp": "203.0.113.45"},
    "result": {"id": "8f2e1c4a", "url": "https://www.speedtest.net/result/c/8f2e1c4a", "persisted": True},
}


class Env:
    """A loaded plugin module wired to a fake Redis, a fake speedtest and in-memory settings."""

    def __init__(self, tmp_path, redis_on=True, run_seconds=0.0):
        ps = load_plugin()
        self.ps = ps
        self.redis = FakeRedis() if redis_on else None
        self.runs = []
        self.run_seconds = run_seconds
        self.settings = {"scheduler_interval": "1", "skip_if_streaming": False}
        self.enabled = True

        def fake_speedtest(logger, server_id=None, auto_update=True):
            self.runs.append(time.time())
            time.sleep(self.run_seconds)
            return dict(FAKE_RESULT)

        ps._get_redis_client = lambda logger: self.redis
        ps._run_speedtest = fake_speedtest
        ps._read_settings_from_db = lambda logger: (self.enabled, dict(self.settings))
        ps._real_prewarm_cli = ps._prewarm_cli
        ps._prewarm_cli = lambda logger, settings: None  # tests opt in via _real_prewarm_cli
        ps._STARTUP_DELAY = 0.05
        ps._TICK_SECONDS = 0.05
        bin_dir = tmp_path / "bin"
        ps.PLUGIN_DATA_DIR = str(tmp_path)
        ps.BIN_DIR = str(bin_dir)
        ps.SPEEDTEST_BIN = str(bin_dir / "speedtest")
        ps.SPEEDTEST_VERSION_FILE = str(bin_dir / "speedtest.version")
        ps.SPEEDTEST_CHECKED_FILE = str(bin_dir / "speedtest.checked")
        self.logger = logging.getLogger("packet_slapper_test")
        self.ctx = {"settings": self.settings, "logger": self.logger}

    def status(self, plugin):
        return plugin.run("scheduler_status", {}, self.ctx)["message"]


def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def watcher_threads():
    return [t for t in threading.enumerate() if t.name == "packet-slapper-scheduler" and t.is_alive()]


@pytest.fixture
def env(tmp_path):
    made = []

    def make(**kw):
        e = Env(tmp_path, **kw)
        made.append(e)
        return e

    yield make
    for e in made:  # stop every watcher a test left running
        if e.ps._controller is not None:
            e.ps._controller.stop.set()
    wait_for(lambda: not watcher_threads(), 3)
