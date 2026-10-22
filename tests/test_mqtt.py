import harness  # noqa: F401  (sets up sys.path, fakes, virtual clock)
import json
import unittest

from harness import Sim, W

import mqttmanager.client as client_module
from mqttmanager import MQTTManager


class Base(unittest.TestCase):
    def setUp(self):
        self.sim = Sim()
        self.b = self.sim.broker

    def tearDown(self):
        self.sim.close()

    def connected(self, mq, conns=None):
        if conns is None:
            return lambda: mq.state == "connected"
        return lambda: mq.state == "connected" and len(self.b.active()) == 1 and len(self.b.conns) == conns


class ConnectionTests(Base):
    def test_connects_after_wifi_with_the_configured_fields(self):
        self.b.users = {"dev": "secret12"}
        mq = self.sim.make(keepalive=30, user="dev", password="secret12")
        self.assertTrue(self.sim.up(mq))
        c = self.b.last.connect
        self.assertEqual((c["client_id"], c["keepalive"], c["clean"], c["user"], c["password"], c["level"]),
                         ("test-client", 30, True, "dev", "secret12", 4))
        self.assertEqual(self.sim.seq(), ["no_wifi", "connecting", "connected"])
        self.assertEqual(self.sim.calls, ["connect"])
        self.assertEqual(self.b.blocked_forever, 0)

    def test_waits_for_wifi_before_touching_the_broker(self):
        W.set_up("Home", False)
        mq = self.sim.make()
        self.sim.run(mq, ms=30000)
        self.assertEqual(mq.state, "no_wifi")
        self.assertEqual(self.b.connect_attempts, [])
        W.set_up("Home", True)
        self.assertTrue(self.sim.up(mq, 120000))

    def test_unknown_host_is_retried_with_backoff(self):
        mq = self.sim.make(server="nowhere.local")
        self.sim.run(mq, ms=10000)
        self.assertEqual(mq.state, "backoff")
        self.assertIn("cannot connect", mq.status()["last_error"])
        self.assertGreaterEqual(mq.status()["failed_attempts"], 2)

    def test_backoff_grows_exponentially_and_is_capped(self):
        self.b.down = True
        mq = self.sim.make(backoff_max=20)
        self.sim.run(mq, ms=200000)
        t = self.b.connect_attempts
        gaps = [(t[i + 1] - t[i]) / 1000 for i in range(len(t) - 1)]
        for n, gap in enumerate(gaps[:7]):
            expect = min(20, 2 * 2 ** n)
            self.assertGreaterEqual(gap, expect * 0.8, "attempt %d" % n)
            self.assertLessEqual(gap, expect * 1.2 + 0.2, "attempt %d" % n)

    def test_bad_credentials_are_reported_and_retried(self):
        self.b.users = {"dev": "right-one"}
        mq = self.sim.make(user="dev", password="wrong-one")
        self.sim.run(mq, ms=20000)
        self.assertNotEqual(mq.state, "connected")
        self.assertIn("bad user name or password", self.sim.log(mq))
        self.b.users = {"dev": "wrong-one"}
        self.assertTrue(self.sim.up(mq, 120000))

    def test_connack_refusal_text(self):
        self.b.connack_code = 5
        mq = self.sim.make()
        self.sim.run(mq, ms=5000)
        self.assertIn("not authorized", mq.status()["last_error"])


class LossTests(Base):
    def test_broker_crash_is_detected_and_subscriptions_restored(self):
        mq = self.sim.make()
        mq.subscribe("cmd/#", lambda t, p: None)
        self.assertTrue(self.sim.up(mq))
        t0 = self.sim.t
        self.b.kill()
        self.assertTrue(self.sim.run(mq, self.connected(mq, 2), 30000))
        self.assertTrue(self.sim.t - t0 < 3000)
        self.assertEqual(self.sim.calls[0], "connect")
        self.assertEqual(self.sim.calls[1][0], "disconnect")
        self.assertEqual(self.sim.calls[2], "connect")
        self.assertEqual(self.b.last.subs, {"cmd/#": 0})

    def test_connection_closed_by_the_broker(self):
        mq = self.sim.make()
        self.assertTrue(self.sim.up(mq))
        self.b.kill("eof")
        self.assertTrue(self.sim.run(mq, lambda: mq.state != "connected", 5000))
        self.assertIn("closed by the broker", self.sim.calls[1][1])

    def test_messages_published_during_an_outage_are_sent_in_order(self):
        mq = self.sim.make()
        self.assertTrue(self.sim.up(mq))
        self.b.kill()
        self.b.down = True
        self.sim.run(mq, ms=5000)
        mids = [mq.publish("t/1", "a"), mq.publish("t/2", "b", qos=1), mq.publish("t/3", "c")]
        self.assertEqual(mq.pending(), 3)
        self.assertTrue(all(mq.is_pending(m) for m in mids))
        self.b.down = False
        self.assertTrue(self.sim.up(mq, 120000))
        self.assertEqual(self.b.topics(), ["t/1", "t/2", "t/3"])

    def test_wifi_loss_closes_and_reconnects_when_wifi_is_back(self):
        mq = self.sim.make(status_topic="dev/status")
        self.assertTrue(self.sim.up(mq))
        W.set_up("Home", False)
        self.assertTrue(self.sim.run(mq, lambda: mq.state == "no_wifi", 5000))
        self.assertIn(("disconnect", "WiFi lost"), self.sim.calls)
        self.assertEqual([m.payload for m in self.b.wills], [b"offline"])
        attempts = len(self.b.connect_attempts)
        self.sim.run(mq, ms=60000)
        self.assertEqual(len(self.b.connect_attempts), attempts)  # no broker attempts without WiFi
        W.set_up("Home", True)
        self.assertTrue(self.sim.run(mq, self.connected(mq, 2), 300000))

    def test_stalled_broker_is_detected_by_keepalive(self):
        mq = self.sim.make(keepalive=20, response_timeout=5)
        self.assertTrue(self.sim.up(mq))
        t0 = self.sim.t
        self.b.stalled = True
        self.assertTrue(self.sim.run(mq, lambda: mq.state != "connected", 60000))
        self.assertLessEqual(self.sim.t - t0, 16000)
        self.assertIn("no answer to ping", self.sim.calls[-1][1])
        self.sim.run(mq, ms=20000)  # reconnect attempts time out waiting for the CONNACK
        self.assertNotEqual(mq.state, "connected")
        self.b.stalled = False
        self.assertTrue(self.sim.run(mq, self.connected(mq), 120000))
        self.assertEqual(self.b.blocked_forever, 0)

    def test_idle_connection_sends_pings_and_stays_up(self):
        mq = self.sim.make(keepalive=20)
        self.assertTrue(self.sim.up(mq))
        self.sim.run(mq, ms=100000)
        self.assertEqual(mq.state, "connected")
        self.assertEqual(len(self.b.conns), 1)
        self.assertTrue(9 <= self.b.pings <= 11, self.b.pings)

    def test_qos1_without_puback_stays_queued_and_is_resent(self):
        mq = self.sim.make(response_timeout=5)
        self.assertTrue(self.sim.up(mq))
        self.sim.run(mq, ms=1000)  # check_msg() has run: umqtt.simple's setblocking(True) must not drop the timeout
        self.b.ack_publish = False
        mid = mq.publish("t", "x", qos=1)
        self.assertEqual(self.b.blocked_forever, 0)
        self.assertTrue(mq.is_pending(mid))
        self.assertEqual(mq.state, "backoff")
        self.b.ack_publish = True
        self.assertTrue(self.sim.up(mq, 30000))
        self.assertFalse(mq.is_pending(mid))
        self.assertEqual(self.b.topics(), ["t", "t"])  # at-least-once

    def test_reconnect_request(self):
        mq = self.sim.make()
        self.assertTrue(self.sim.up(mq))
        mq.reconnect()
        self.assertTrue(self.sim.run(mq, self.connected(mq, 2), 10000))
        self.assertFalse(self.b.conns[0].graceful)


class PublishTests(Base):
    def test_payload_types_qos_retain_and_on_delivered(self):
        delivered = []
        mq = self.sim.make(on_delivered=delivered.append)
        self.assertTrue(self.sim.up(mq))
        mids = [mq.publish("a", "x"), mq.publish("b", {"v": 1}, qos=1, retain=True), mq.publish("c", 21.5),
                mq.publish("d", None), mq.publish("e", b"\x00\xff"), mq.publish("f", bytearray(b"ba"))]
        self.assertEqual(delivered, mids)
        self.assertEqual(mq.pending(), 0)
        got = [(m.topic, m.payload, m.qos, m.retain) for m in self.b.published]
        self.assertEqual(got, [("a", b"x", 0, False), ("b", b'{"v": 1}', 1, True), ("c", b"21.5", 0, False),
                               ("d", b"", 0, False), ("e", b"\x00\xff", 0, False), ("f", b"ba", 0, False)])
        self.assertEqual(self.b.retained["b"], (b'{"v": 1}', 1))

    def test_invalid_publish_is_refused(self):
        mq = self.sim.make()
        for topic in ("a/+", "a/#", "", None):
            self.assertRaises(ValueError, mq.publish, topic, "x")
        self.assertRaises(ValueError, mq.publish, "a", "x", qos=2)
        self.assertRaises(ValueError, mq.subscribe, "a", qos=2)

    def test_full_queue_drops_the_oldest(self):
        self.b.down = True
        mq = self.sim.make(queue_size=3)
        self.sim.run(mq, ms=3000)
        mids = [mq.publish("t/%d" % i, i) for i in range(5)]
        self.assertEqual(mq.pending(), 3)
        self.assertEqual(mq.status()["dropped"], 2)
        self.assertFalse(mq.is_pending(mids[0]))
        self.assertIn("queue full", self.sim.log(mq))
        self.b.down = False
        self.assertTrue(self.sim.up(mq, 120000))
        self.assertEqual(self.b.topics(), ["t/2", "t/3", "t/4"])

    def test_large_payloads_and_short_reads(self):
        got = []
        mq = self.sim.make()
        mq.subscribe("big/#", lambda t, p: got.append(p))
        self.assertTrue(self.sim.up(mq))
        self.b.read_chunk = 7
        for size in (300, 20000):
            mq.publish("out/%d" % size, b"x" * size, qos=1)
            self.b.inject("big/%d" % size, b"y" * size, qos=1)
        self.sim.run(mq, ms=2000)
        self.assertEqual([len(m.payload) for m in self.b.published], [300, 20000])
        self.assertEqual([len(p) for p in got], [300, 20000])
        self.assertEqual(mq.state, "connected")

    def test_packet_id_wraps_around(self):
        mq = self.sim.make()
        self.assertTrue(self.sim.up(mq))
        mq._client.pid = 65534
        for i in range(3):
            mq.publish("t", i, qos=1)
        self.assertEqual(mq.pending(), 0)
        self.assertEqual(mq.state, "connected")
        self.assertEqual(mq._client.pid, 2)


class ReceiveTests(Base):
    def test_handlers_wildcards_and_on_message(self):
        mine, other = [], []
        mq = self.sim.make(on_message=lambda t, p: other.append((t, p)))
        mq.subscribe("sensors/+/temp", lambda t, p: mine.append((t, p)))
        mq.subscribe("alerts/#", qos=1)
        self.assertTrue(self.sim.up(mq))
        self.b.inject("sensors/kitchen/temp", "21")
        self.b.inject("alerts/fire/now", "!", qos=1)
        self.b.inject("sensors/kitchen/hum", "40")
        self.sim.run(mq, ms=1000)
        self.assertEqual(mine, [("sensors/kitchen/temp", b"21")])
        self.assertEqual(other, [("alerts/fire/now", b"!")])
        self.assertEqual(self.b.last.acked, [1])  # QoS 1 message acknowledged

    def test_failing_handler_does_not_stop_the_loop(self):
        got = []

        def handler(topic, payload):
            if payload == b"bad":
                raise ValueError("bad value")
            got.append(payload)
        mq = self.sim.make()
        mq.subscribe("x", handler)
        self.assertTrue(self.sim.up(mq))
        self.b.inject("x", "bad")
        self.b.inject("x", "good")
        self.sim.run(mq, ms=1000)
        self.assertEqual(got, [b"good"])
        self.assertIn("handler for 'x' failed: ValueError: bad value", self.sim.log(mq))
        self.assertEqual(mq.state, "connected")

    def test_async_handler_in_blocking_mode(self):
        got = []

        async def handler(topic, payload):
            got.append(payload)
            return "done"
        mq = self.sim.make()
        mq.subscribe("x", handler, reply=True)
        self.assertTrue(self.sim.up(mq))
        self.b.inject("x", "go")
        self.sim.run(mq, ms=1000)
        self.assertEqual(got, [b"go"])
        self.assertEqual(self.b.published[-1].payload, b"done")

    def test_retained_message_arrives_on_subscribe(self):
        got = []
        self.b.inject("cfg/interval", "30", retain=True)
        mq = self.sim.make()
        mq.subscribe("cfg/#", lambda t, p: got.append((t, p)))
        self.assertTrue(self.sim.up(mq))
        self.sim.run(mq, ms=500)
        self.assertEqual(got, [("cfg/interval", b"30")])

    def test_subscribe_while_connected_then_unsubscribe(self):
        got = []
        mq = self.sim.make()
        self.assertTrue(self.sim.up(mq))
        mq.subscribe("late/one", lambda t, p: got.append(p), qos=1)
        self.assertEqual(self.b.last.subs, {"late/one": 1})
        self.b.inject("late/one", "1")
        self.sim.run(mq, ms=500)
        self.assertTrue(mq.unsubscribe("late/one"))
        self.assertEqual(self.b.last.subs, {})
        self.b.inject("late/one", "2")
        self.sim.run(mq, ms=500)
        self.assertEqual(got, [b"1"])
        self.assertFalse(mq.unsubscribe("late/one"))
        self.assertEqual(mq.subscriptions(), [])

    def test_unexpected_packets_are_skipped_without_losing_sync(self):
        got = []
        mq = self.sim.make()
        mq.subscribe("x", lambda t, p: got.append((t, p)))
        self.assertTrue(self.sim.up(mq))
        # a stray SUBACK whose packet id bytes look like a PUBLISH header if left unread
        self.b.last.out += b"\x90\x03\x30\x31\x00" + b"\x40\x02\x00\x07"
        self.b.inject("x", "after")
        self.sim.run(mq, ms=1000)
        self.assertEqual(got, [("x", b"after")])
        self.assertEqual((mq.state, len(self.b.conns)), ("connected", 1))

    def test_refused_subscription_keeps_the_connection(self):
        self.b.refuse = ("secret/#",)
        mq = self.sim.make()
        mq.subscribe("secret/#")
        mq.subscribe("ok/#")
        self.assertTrue(self.sim.up(mq))
        self.sim.run(mq, ms=2000)
        self.assertEqual(mq.state, "connected")
        self.assertIn("refused the subscription to 'secret/#'", self.sim.log(mq))
        self.assertEqual(self.b.last.subs, {"ok/#": 0})


class ReplyTests(Base):
    def test_reply_goes_to_topic_plus_suffix_and_is_not_answered_again(self):
        calls = []

        def cmd(topic, payload):
            calls.append(topic)
            return "ok:" + payload.decode()
        mq = self.sim.make()
        mq.subscribe("dev/cmd/#", cmd, qos=1, reply=True)
        self.assertTrue(self.sim.up(mq))
        self.b.inject("dev/cmd/led", "on", qos=1)
        self.sim.run(mq, ms=1000)
        m = self.b.published[-1]
        self.assertEqual((m.topic, m.payload, m.qos), ("dev/cmd/led/reply", b"ok:on", 1))
        self.assertEqual(calls, ["dev/cmd/led"])  # the reply came back through '#' but was not handled

    def test_fixed_reply_topic_and_json(self):
        mq = self.sim.make()
        mq.subscribe("dev/get", lambda t, p: {"rssi": -60}, reply="dev/state")
        self.assertTrue(self.sim.up(mq))
        self.b.inject("dev/get", "")
        self.sim.run(mq, ms=1000)
        self.assertEqual([(m.topic, json.loads(m.payload)) for m in self.b.published], [("dev/state", {"rssi": -60})])

    def test_none_means_no_reply_and_errors_are_replied(self):
        mq = self.sim.make(reply_suffix="/ack")

        @mq.on("dev/cmd/+", reply=True)
        def cmd(topic, payload):
            if payload == b"boom":
                raise ValueError("bad command")
            return None
        self.assertTrue(self.sim.up(mq))
        self.b.inject("dev/cmd/a", "quiet")
        self.b.inject("dev/cmd/b", "boom")
        self.sim.run(mq, ms=1000)
        self.assertEqual([(m.topic, json.loads(m.payload)) for m in self.b.published],
                         [("dev/cmd/b/ack", {"error": "ValueError: bad command"})])


class StatusTests(Base):
    def test_status_topic_sets_will_and_birth(self):
        mq = self.sim.make(status_topic="dev/status")
        self.assertTrue(self.sim.up(mq))
        self.assertEqual(self.b.last.connect["will"], ("dev/status", b"offline", True, 1))
        m = self.b.published[0]
        self.assertEqual((m.topic, m.payload, m.qos, m.retain), ("dev/status", b"online", 1, True))
        self.b.kill()
        self.assertEqual(self.b.retained["dev/status"], (b"offline", 1))
        self.assertTrue(self.sim.run(mq, self.connected(mq, 2), 10000))
        self.assertEqual(self.b.retained["dev/status"], (b"online", 1))

    def test_stop_says_offline_and_disconnects_cleanly(self):
        mq = self.sim.make(status_topic="dev/status")
        self.assertTrue(self.sim.up(mq))
        mq.stop()
        self.assertEqual(mq.state, "idle")
        self.assertEqual(self.b.published[-1].payload, b"offline")
        self.assertTrue(self.b.last.graceful)
        self.assertEqual(self.b.wills, [])

    def test_custom_will(self):
        mq = self.sim.make(will=("dev/lwt", "gone"))
        self.assertTrue(self.sim.up(mq))
        self.assertEqual(self.b.last.connect["will"], ("dev/lwt", b"gone", False, 0))
        self.assertRaises(ValueError, self.sim.make, status_topic="a", will=("b", "c"))

    def test_status_dict_and_shared_log(self):
        mq = self.sim.make()
        mq.subscribe("a/#")
        self.assertTrue(self.sim.up(mq))
        st = mq.status()
        self.assertEqual((st["state"], st["server"], st["port"], st["client_id"], st["subscriptions"]),
                         ("connected", "broker.local", 1883, "test-client", ["a/#"]))
        self.assertTrue(any(" mqtt: " in line for line in mq.wifi.log.tail()))
        mq2 = self.sim.make(share_log=False)
        self.assertIsNot(mq2.log._lines, mq2.wifi.log._lines)

    def test_default_client_id(self):
        mq = MQTTManager("broker.local", wifi=self.sim.wifi())
        self.assertTrue(mq.client_id.startswith("esp32-"))


class DriverTests(Base):
    def test_blocking_connect_check_and_flush(self):
        got = []
        mq = self.sim.make()
        mq.subscribe("in", lambda t, p: got.append(p))
        self.assertTrue(mq.connect())
        self.assertEqual(mq.state, "connected")
        mq.publish("out", "1", qos=1)
        self.assertEqual(self.b.topics(), ["out"])  # sent right away
        self.b.inject("in", "hello")
        for _ in range(3):
            mq.check()
            self.sim.advance(100)
        self.assertEqual(got, [b"hello"])
        self.b.kill()
        self.b.down = True
        mq.check()
        mid = mq.publish("out", "2")
        self.assertTrue(mq.is_pending(mid))
        t0 = self.sim.t
        self.assertFalse(mq.flush(timeout=5))
        self.assertTrue(5000 <= self.sim.t - t0 <= 6000)
        self.b.down = False
        self.assertTrue(mq.flush(timeout=60))
        self.assertEqual(self.b.topics(), ["out", "out"])

    def test_blocking_connect_timeout(self):
        self.b.down = True
        mq = self.sim.make()
        t0 = self.sim.t
        self.assertFalse(mq.connect(timeout=10))
        self.assertTrue(10000 <= self.sim.t - t0 <= 11500, self.sim.t - t0)


class _TLSSock:
    def __init__(self, sock, log, how):
        self.s = sock
        log.append(how)

    def __getattr__(self, name):
        return getattr(self.s, name)


class TLSTests(Base):
    def test_ssl_true_is_encrypted_but_unverified(self):
        made = []

        class Ctx:
            def __init__(self, protocol):
                self.protocol = protocol
                self.verify_mode = "default"
                made.append(self)

            def wrap_socket(self, sock, server_hostname=None):
                return _TLSSock(sock, self.log, server_hostname)

        class FakeSSL:
            PROTOCOL_TLS_CLIENT = 16
            CERT_NONE = 0
            SSLContext = Ctx
        Ctx.log = []
        saved = client_module.ssl
        client_module.ssl = FakeSSL
        try:
            mq = self.sim.make(ssl=True)
            self.assertTrue(self.sim.up(mq))
        finally:
            client_module.ssl = saved
        self.assertEqual([(c.protocol, c.verify_mode) for c in made], [(16, 0)])
        self.assertEqual((Ctx.log, mq.status()["port"]), (["broker.local"], 8883))
        self.assertTrue("without certificate verification" in self.sim.log(mq))

    def test_ssl_context_and_timeouts_under_tls(self):
        log = []

        class Ctx:
            def wrap_socket(self, sock, server_hostname=None):
                return _TLSSock(sock, log, server_hostname)
        mq = self.sim.make(ssl=Ctx(), port=8884, response_timeout=5)
        self.assertTrue(self.sim.up(mq))
        mq.publish("t", "x", qos=1)
        self.assertEqual((log, self.b.topics(), mq.status()["port"]), (["broker.local"], ["t"], 8884))
        # a TLS socket has no settimeout(): the timeout must survive setblocking(True) on the TCP socket
        self.b.ack_publish = False
        mq.publish("t", "y", qos=1)
        self.assertEqual(mq.state, "backoff")
        self.assertEqual(self.b.blocked_forever, 0)


if __name__ == "__main__":
    unittest.main()
