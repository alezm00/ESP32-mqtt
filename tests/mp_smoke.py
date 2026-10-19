"""Smoke test that also runs on the real MicroPython interpreter (unix port), fake radio + fake broker.

From the repository root:

    MICROPYPATH=.frozen:tests/vendor/umqtt-1.8.1:tests/fakes:deps/ESP32-wifi/tests/fakes:deps/ESP32-wifi:. \
        micropython tests/mp_smoke.py
    PYTHONPATH=tests/vendor/umqtt-1.8.1:tests/fakes:deps/ESP32-wifi/tests/fakes:deps/ESP32-wifi:. \
        python3 tests/mp_smoke.py
"""
import os
import sys

import network
from broker import Broker
from wifimanager import WiFiManager, compat
import mqttmanager.client as client_module
from mqttmanager import MQTTManager

W = network.WORLD
TMP = os.getenv("WM_TMP") or "/tmp"
CFG = TMP + "/mq_smoke_wifi.json"
failures = []


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond:
        failures.append(name)


def cleanup():
    for suffix in ("", ".tmp", ".bad"):
        try:
            os.remove(CFG + suffix)
        except OSError:
            pass


def make_wifi():
    cleanup()
    wm = WiFiManager(None, config_file=CFG, config_mode="never")
    wm.log.console = False
    wm.add_network("Home", "homepass12")
    return wm


# ---- part 1: both supervisors on a virtual clock --------------------------------------------
clock = [100000]


def advance(ms):
    clock[0] += ms


def drive(mq, until, limit_ms):
    wm = mq.wifi
    wm._boot()
    mq._boot()
    end = clock[0] + limit_ms
    wifi_at = mq_at = clock[0]
    while clock[0] < end:
        if clock[0] >= wifi_at:
            wifi_at = clock[0] + wm._next()
        if mq._kick:
            mq._kick = False
            mq_at = clock[0]
        if clock[0] >= mq_at:
            mq_at = clock[0] + mq._step()
        if until():
            return True
        clock[0] = max(clock[0], min(wifi_at, mq_at, end))
    return until()


def virtual_part():
    compat.ticks_ms = lambda: clock[0]
    compat.sleep_ms_blocking = advance
    compat.run_for = advance
    W.reset()
    W.add_ap("Home", "homepass12", delay_ms=1000)
    b = Broker(lambda: clock[0], advance)
    client_module.socket = b.socket_module()
    events = []
    got = []

    def led(topic, payload):
        got.append(payload)
        if payload == b"boom":
            raise ValueError("bad command")
        return {"led": payload.decode()}

    mq = MQTTManager("broker.local", wifi=make_wifi(), client_id="smoke", keepalive=20, response_timeout=5,
                     status_topic="dev/status", queue_size=3,
                     on_connect=lambda: events.append("up"), on_disconnect=lambda r: events.append(("down", r)))
    mq.log.console = False
    mq.subscribe("dev/cmd/#", led, qos=1, reply=True)

    def up():
        return mq.state == "connected" and not mq.pending()

    check("connects after WiFi", drive(mq, up, 60000) and events == ["up"])
    check("will and birth", b.last.connect["will"] == ("dev/status", b"offline", True, 1)
          and b.retained["dev/status"] == (b"online", 1))
    mid = mq.publish("dev/data", {"t": 21.5}, qos=1)
    check("QoS 1 publish delivered", not mq.is_pending(mid) and b.published[-1].payload == b'{"t": 21.5}')

    b.inject("dev/cmd/led", "on", qos=1)
    b.inject("dev/cmd/led", "boom")
    drive(mq, lambda: False, 1000)
    replies = [(m.topic, m.payload) for m in b.published if m.topic.endswith("/reply")]
    check("command handler + reply", got == [b"on", b"boom"] and replies[0] == ("dev/cmd/led/reply", b'{"led": "on"}'))
    check("error reply", replies[1] == ("dev/cmd/led/reply", b'{"error": "ValueError: bad command"}'))

    b.kill()
    check("loss detected, reconnects, resubscribes",
          drive(mq, lambda: up() and len(b.conns) == 2, 30000) and b.last.subs == {"dev/cmd/#": 1}
          and events[1][0] == "down" and b.wills[0].payload == b"offline")

    b.kill()
    b.down = True
    drive(mq, lambda: False, 3000)
    for i in range(5):
        mq.publish("q/%d" % i, i)
    check("offline queue keeps the newest", mq.pending() == 3 and mq.status()["dropped"] == 2)
    b.down = False
    check("queue flushed after reconnect", drive(mq, up, 120000) and b.topics()[-3:] == ["q/2", "q/3", "q/4"])

    W.set_up("Home", False)
    check("WiFi loss -> no_wifi", drive(mq, lambda: mq.state == "no_wifi", 5000))
    W.set_up("Home", True)
    check("back when WiFi returns", drive(mq, up, 300000))

    b.stalled = True
    check("stalled broker found by keepalive", drive(mq, lambda: mq.state != "connected", 30000)
          and "no answer to ping" in events[-1][1])
    b.stalled = False
    check("recovers from the stall", drive(mq, up, 120000))
    mq.stop()
    check("clean stop", b.last.graceful and b.published[-1].payload == b"offline")
    check("no read without timeout", b.blocked_forever == 0)

    # blocking driver
    b.reset()
    mq2 = MQTTManager("broker.local", wifi=make_wifi(), client_id="smoke2")
    mq2.log.console = False
    check("blocking connect()", mq2.connect(timeout=60) and mq2.state == "connected")
    mq2.publish("x", "1")
    check("blocking flush()", mq2.flush(timeout=5) and b.topics() == ["x"])
    mq2.stop()


# ---- part 2: asyncio driver, real clock -----------------------------------------------------
async def async_part():
    import asyncio
    import time
    compat.ticks_ms = time.ticks_ms if hasattr(time, "ticks_ms") else (lambda: int(time.monotonic() * 1000))
    W.reset()
    W.add_ap("Home", "homepass12", delay_ms=50)
    b = Broker(compat.ticks_ms, lambda ms: None)
    client_module.socket = b.socket_module()
    wm = make_wifi()
    mq = MQTTManager("broker.local", wifi=wm, client_id="smoke3", poll_ms=20)
    mq.log.console = False

    async def echo(topic, payload):
        await asyncio.sleep_ms(10) if hasattr(asyncio, "sleep_ms") else await asyncio.sleep(0.01)
        return payload.decode().upper()

    mq.subscribe("cmd/echo", echo, reply=True)
    check("async start()", await mq.start())
    mid = mq.publish("t", "1", qos=1)
    check("async wait_delivered()", await mq.wait_delivered(mid, timeout=2))
    b.inject("cmd/echo", "hi")
    for _ in range(100):
        if "cmd/echo/reply" in b.topics():
            break
        await compat.sleep_ms(20)
    check("async handler reply", b.published[-1].payload == b"HI")
    b.kill()
    await compat.sleep_ms(100)
    check("async reconnect", await mq.wait_connected(timeout=5) and len(b.conns) == 2)
    mq.stop()
    wm.stop()
    await compat.sleep_ms(100)


virtual_part()
import asyncio  # noqa: E402

asyncio.run(async_part())
cleanup()
print("---")
print("FAILED: %s" % failures if failures else "ALL PASSED")
sys.exit(1 if failures else 0)
