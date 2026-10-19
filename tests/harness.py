"""Virtual-clock harness: WiFiManager on the fake radio + MQTTManager on the fake broker, no sleeping."""
import _setup  # noqa: F401
import os
import shutil
import tempfile

import network
from broker import Broker
from wifimanager import WiFiManager, compat
import mqttmanager.client as client_module
from mqttmanager import MQTTManager

_orig = (compat.ticks_ms, compat.sleep_ms_blocking, compat.run_for, client_module.socket)
W = network.WORLD
SERVER = "broker.local"


class Sim:
    def __init__(self):
        self.t = 100000
        compat.ticks_ms = lambda: self.t
        compat.sleep_ms_blocking = self.advance
        compat.run_for = self.advance
        W.reset()
        W.add_ap("Neighbor", "neighborpw", rssi=-90)   # scans are never empty (an empty scan = "scan failed")
        W.add_ap("Home", "homepass12", delay_ms=1000)
        self.broker = Broker(lambda: self.t, self.advance, hosts=(SERVER,))
        client_module.socket = self.broker.socket_module()
        self.dir = tempfile.mkdtemp()
        self.states = []
        self.calls = []

    def close(self):
        compat.ticks_ms, compat.sleep_ms_blocking, compat.run_for, client_module.socket = _orig
        shutil.rmtree(self.dir, ignore_errors=True)

    def advance(self, ms):
        self.t += ms

    def wifi(self, **kw):
        kw.setdefault("config_file", os.path.join(self.dir, "wifi_config.json"))
        kw.setdefault("config_mode", "never")
        kw.setdefault("log_level", "DEBUG")
        wm = WiFiManager(None, **kw)
        wm.log.console = False
        wm.add_network("Home", "homepass12")
        return wm

    def make(self, wifi=None, server=SERVER, **kw):
        kw.setdefault("client_id", "test-client")
        kw.setdefault("log_level", "DEBUG")
        kw.setdefault("on_state", lambda old, new: self.states.append(new))
        kw.setdefault("on_connect", lambda: self.calls.append("connect"))
        kw.setdefault("on_disconnect", lambda reason: self.calls.append(("disconnect", reason)))
        mq = MQTTManager(server, wifi=wifi or self.wifi(), **kw)
        mq.log.console = False
        return mq

    def run(self, mq, until=None, ms=60000):
        """Drive both supervisors on virtual time until `until()` is true or `ms` elapsed."""
        wm = mq.wifi
        wm._boot()
        mq._boot()
        end = self.t + ms
        wifi_at = mq_at = self.t
        for _ in range(1000000):
            if self.t >= wifi_at:
                wifi_at = self.t + wm._next()
            if mq._kick:
                mq._kick = False
                mq_at = self.t
            if self.t >= mq_at:
                mq_at = self.t + mq._step()
            if until is not None and until():
                return True
            if self.t >= end:
                break
            self.t = max(self.t, min(wifi_at, mq_at, end))
        return until is None

    def up(self, mq, ms=60000):
        """Run until WiFi and broker are connected and the queue is empty."""
        return self.run(mq, lambda: mq.state == "connected" and not mq.pending(), ms)

    def seq(self):
        out = []
        for s in self.states:
            if not out or out[-1] != s:
                out.append(s)
        return out

    def log(self, mq):
        return "\n".join(mq.log.tail())
