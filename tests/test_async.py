"""The asyncio driver on a real event loop and the real clock (fake radio, fake broker)."""
import _setup  # noqa: F401
import asyncio
import unittest

import network
import umqtt.simple as umqtt_module
from _util import rmtree, tmpdir
from broker import Broker
from wifimanager import WiFiManager, compat
from mqttmanager import MQTTManager


class AsyncTests(unittest.TestCase):
    def setUp(self):
        network.WORLD.reset()
        network.WORLD.add_ap("Home", "homepass12", delay_ms=50)
        self.broker = Broker(compat.ticks_ms, lambda ms: None)
        self._socket = umqtt_module.socket
        umqtt_module.socket = self.broker.socket_module()
        self.dir = tmpdir()

    def tearDown(self):
        umqtt_module.socket = self._socket
        rmtree(self.dir)

    async def until(self, cond, ms=3000):
        for _ in range(ms // 20):
            if cond():
                return True
            await asyncio.sleep(0.02)
        return cond()

    def make(self, **kw):
        wm = WiFiManager(None, config_file=self.dir + "/wifi.json", config_mode="never")
        wm.log.console = False
        wm.add_network("Home", "homepass12")
        mq = MQTTManager("broker.local", wifi=wm, client_id="async", poll_ms=20, **kw)
        mq.log.console = False
        return mq

    def test_background_supervision(self):
        b = self.broker
        events = []

        async def echo(topic, payload):
            await asyncio.sleep(0.01)
            return payload.decode().upper()

        async def main():
            mq = self.make(on_connect=lambda: events.append("up"),
                           on_disconnect=lambda reason: events.append("down"))
            mq.subscribe("cmd/echo", echo, qos=1, reply=True)
            self.assertTrue(await mq.start())                 # also starts the WiFiManager
            self.assertTrue(mq.wifi.is_connected())

            mid = mq.publish("t/x", "1", qos=1)
            self.assertTrue(await mq.wait_delivered(mid, timeout=2))

            b.inject("cmd/echo", "hi", qos=1)                 # async handler, reply published by a task
            self.assertTrue(await self.until(lambda: "cmd/echo/reply" in b.topics()))
            self.assertEqual(b.published[-1].payload, b"HI")

            b.kill()                                          # loss detected and repaired in the background
            b.down = True
            self.assertTrue(await self.until(lambda: mq.state != "connected"))
            mid = mq.publish("t/y", "2")
            self.assertFalse(await mq.wait_delivered(mid, timeout=0.3))
            b.down = False
            self.assertTrue(await mq.wait_connected(timeout=10))
            self.assertTrue(await mq.wait_delivered(timeout=2))
            self.assertEqual(b.topics()[-1], "t/y")
            self.assertEqual(b.last.subs, {"cmd/echo": 1})
            self.assertEqual(events, ["up", "down", "up"])

            mq.stop()
            mq.wifi.stop()
            await asyncio.sleep(0.1)
            self.assertTrue(b.last.graceful)

        asyncio.run(main())

    def test_start_without_wifi_returns_false(self):
        network.WORLD.set_up("Home", False)

        async def main():
            mq = self.make()
            mq.wifi.connect_timeout = 1
            mq.wifi.retries_per_network = 1
            self.assertFalse(await mq.start())
            self.assertEqual(mq.state, "no_wifi")
            self.assertEqual(self.broker.connect_attempts, [])
            mq.stop()
            mq.wifi.stop()
            await asyncio.sleep(0.1)

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
