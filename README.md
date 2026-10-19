# mqttmanager — plug-and-play MQTT for MicroPython on ESP32

The MQTT companion of [ESP32-wifi](https://github.com/alezm00/ESP32-wifi) (`wifimanager`). It uses it for the network, and
`umqtt.simple` (already in the official ESP32 firmware) for the MQTT packets. It gives your project:

* **Connection to the broker** as soon as WiFi is up (user/password, TLS, keepalive, clean or persistent session)
* **Loss detection** (socket errors, broker closing, WiFi gone, stalled broker found by keepalive pings) and **reconnection** with exponential backoff (+ jitter)
* **Sending** with an **offline queue**: messages published while offline wait in RAM and go out, in order, after reconnecting
* **Delivery confirmation**: every `publish()` returns an id; QoS 1 is "delivered" when the broker's PUBACK arrives (`on_delivered`, `wait_delivered()`, `flush()`)
* **Receiving**: subscriptions with `+`/`#` wildcards and a handler per topic, restored at every reconnection
* **Commands → replies**: what a handler returns is published as the reply (`<topic>/reply` or a topic you choose)
* **Online/offline presence** (`status_topic`): retained `online` at connection, `offline` as Last Will
* **Logs** to the console and into the WiFi manager's log (so they show up in its config page too)

Works with `asyncio` (background supervision) **and** with plain blocking scripts, exactly like `wifimanager`.

---

## 1. Install

With `mip`, which installs `mqttmanager` **and its dependency `wifimanager`** into `/lib`:

```bash
pip install mpremote
mpremote mip install github:alezm00/ESP32-mqtt
```

Or by hand. Clone with the submodule, then copy both folders:

```bash
git clone --recurse-submodules https://github.com/alezm00/ESP32-mqtt
cd ESP32-mqtt
mpremote fs mkdir :lib
mpremote cp -r deps/ESP32-wifi/wifimanager :lib/
mpremote cp -r mqttmanager :lib/
```

`umqtt.simple` is frozen in the official ESP32 firmware, so there is nothing to install. On a custom firmware without it, run
`mpremote mip install umqtt.simple`. Every version shipped since MicroPython 1.20 works: 1.3.4, 1.4.0, 1.6.0 and 1.8.1 differ in their
APIs, and `mqttmanager` handles the differences (all four are tested).

Requirements: MicroPython ≥ 1.20 for ESP32 (ESP32, S2, S3, C3). Flash use ≈ 35 kB of `.py`, ≈ 12 kB compiled with `mpy-cross`.

## 2. Quick start

```python
# main.py — asyncio
import asyncio
from wifimanager import WiFiManager
from mqttmanager import MQTTManager

wm = WiFiManager(networks=[("MyHome", "my-password")])
mq = MQTTManager("192.168.1.10", wifi=wm, status_topic="home/esp32/status")

@mq.on("home/esp32/led")
def led(topic, payload):
    print("LED ->", payload)

async def main():
    await mq.start()            # starts the WiFi manager too; both are supervised in the background
    while True:
        mq.publish("home/esp32/temp", 21.5)   # sent now, or queued while offline
        await asyncio.sleep(10)

asyncio.run(main())
```

Blocking version:

```python
from wifimanager import WiFiManager
from mqttmanager import MQTTManager

wm = WiFiManager(networks=[("MyHome", "my-password")])
mq = MQTTManager("broker.local", wifi=wm, on_message=lambda topic, payload: print(topic, payload))
mq.subscribe("home/esp32/#")
mq.connect()                    # blocks until WiFi and the broker are up

while True:
    mq.check()                  # reads messages, pings, detects loss, reconnects; also runs wm.check()
    ...                         # your code (mq.publish(...) sends immediately when connected)
```

Without `wifi=`, a `WiFiManager()` with default settings is created for you (saved networks + config page when none works).
See `examples/` for complete scripts (asyncio, blocking, commands with replies).

## 3. Sending and delivery confirmation

```python
mid = mq.publish("home/esp32/data", {"t": 21.5, "h": 40}, qos=1, retain=False)
```

* The **payload** can be `str` (UTF-8), `bytes`, `None` (empty message, e.g. to clear a retained topic), or anything `json.dumps()` accepts
  (dict, list, numbers, booleans), which is sent as JSON.
* **QoS 0 or 1** (umqtt.simple has no QoS 2). QoS 0 is "delivered" when it is written to the socket. QoS 1 is delivered when the broker's PUBACK arrives.
* `publish()` **never fails because you are offline**. When connected it sends right away. Otherwise the message joins a RAM queue
  (`queue_size`, default 20) that is sent, in order, as soon as the connection is back. When the queue is full the **oldest** message is dropped
  (a warning in the log, `status()["dropped"]` counts them).
* A QoS 1 message whose PUBACK does not arrive within `response_timeout` stays in the queue. The connection is considered broken and
  the message is sent again after reconnecting (*at least once*: the broker may receive it twice).

| How to know it arrived | |
|---|---|
| `on_delivered(mid)` | callback for every delivered message |
| `await mq.wait_delivered(mid, timeout=None)` | asyncio. `True` once delivered, `False` on timeout or if it was dropped. Without `mid`: waits for the whole queue |
| `mq.flush(timeout=None)` | blocking: runs the connection until the queue is empty (`False` after `timeout` s) |
| `mq.is_pending(mid)`, `mq.pending()` | still queued? how many are queued? |

## 4. Receiving, commands and replies

```python
mq.subscribe("sensors/+/temp", handler, qos=1)      # handler(topic: str, payload: bytes)
mq.subscribe("alerts/#")                             # no handler: goes to on_message(topic, payload)

@mq.on("devices/kitchen/cmd/led", qos=1, reply=True)
def cmd_led(topic, payload):
    led.value(payload == b"on")
    return {"led": led.value()}                      # published on devices/kitchen/cmd/led/reply
```

* Subscriptions are remembered and **made again at every reconnection**. Subscribing while connected takes effect at once.
  `mq.unsubscribe(topic)` removes one. A subscription refused by the broker is logged and the connection stays up.
* Handlers can be plain functions or `async def`. A handler that raises an exception is logged and never stops the loop. Several matching
  subscriptions each get the message. Messages no handler wants go to `on_message`.
* `reply` controls what happens to the handler's return value:

| `reply=` | the return value is published on |
|---|---|
| `None` / `False` (default) | nowhere (ignored) |
| `True` | the incoming topic + `reply_suffix` (default `"/reply"`) |
| `"some/topic"` | that fixed topic |

  The reply uses the subscription's QoS and the same payload rules as `publish()`. A handler returning `None` sends no reply.
  A handler that raises replies `{"error": "ValueError: ..."}`, so the sender is not left waiting. Replies are never fed back to a replying handler
  (no loops with `#` filters).

Test from a PC: `mosquitto_sub -v -t 'devices/kitchen/#'` in one terminal, `mosquitto_pub -t devices/kitchen/cmd/led -m on` in another.

## 5. Connection loss and reconnection — how it works

The supervisor polls every `poll_ms` (100 ms) while connected and reads whatever the broker sent, without blocking.

1. **WiFi first.** Nothing is attempted while `wm.is_connected()` is false (state `no_wifi`). If WiFi drops, the broker socket is closed
   immediately (`on_disconnect("WiFi lost")`). When it comes back the broker is contacted at once.
2. **Loss detection.** A connection is considered lost on:
   * a socket error or the broker closing the connection;
   * a QoS 1 / SUBACK answer that does not arrive within `response_timeout`;
   * a PINGREQ with no answer within `response_timeout`. A PINGREQ is sent after `keepalive/2` s without outgoing traffic, so a broker that silently vanished
     (half-open TCP, router reboot) is found in at most `keepalive/2 + response_timeout` s.
3. **On loss** the socket is closed *without* DISCONNECT (so the broker publishes your Last Will) and `on_disconnect(reason)` is called. A new
   attempt follows after about 1 s.
4. **Failed attempts** use **exponential backoff + jitter**: 2 → 4 → 8 … ≤ `backoff_max` s (±20 %). The reason is logged: unreachable host, refused,
   or the broker's CONNACK answer (`bad user name or password`, `not authorized`, …).
5. **After reconnecting:** the `online` status is published, subscriptions are restored, the queue is sent, and `on_connect()` is called.

Every blocking socket read has a timeout (`connect_timeout` while connecting, `response_timeout` afterwards). umqtt.simple on its own can
block forever when a broker stops answering in the middle of a packet; `mqttmanager` makes that impossible.

**Things that block for a moment** (umqtt.simple is a blocking library):
* DNS + connecting: up to `connect_timeout`;
* a QoS 1 publish, a subscribe: one round trip to the broker.

In asyncio mode other tasks wait during that time. In blocking mode, a WiFi config portal opened *while connected* (button) is served
inside `wm.check()`, so the broker may drop you meanwhile. Use asyncio if that matters.

## 6. Constructor reference

`MQTTManager(server, *, ...)` — everything after `server` is keyword-only.

**Broker**

| Option | Default | Meaning |
|---|---|---|
| `server` | — | broker host name or IP |
| `wifi` | `None` | the `WiFiManager` to use (`None` creates one). Any object with `is_connected()` works |
| `port` | `0` | `0` = 1883, or 8883 with TLS |
| `client_id` | `None` | default `esp32-<unique id>` (must be unique on the broker) |
| `user` / `password` | `None` | broker credentials |
| `keepalive` | `60` | seconds (sent in CONNECT); pings after `keepalive/2` s of silence. `0` disables |
| `clean_session` | `True` | `False` keeps the session (QoS 1 messages for you are kept by the broker while offline) |
| `ssl` / `ssl_params` | `False` / `None` | `True` = TLS via `ssl.wrap_socket(**ssl_params)` (SNI added automatically), or an `ssl.SSLContext` (MicroPython ≥ 1.23) |
| `connect_timeout` | `10` | seconds for TCP + TLS + CONNACK |
| `response_timeout` | `10` | seconds to wait for PUBACK / SUBACK / ping answer / the rest of a packet |

**Presence / Last Will**

| Option | Default | Meaning |
|---|---|---|
| `status_topic` | `None` | e.g. `"home/esp32/status"`: retained `status_online` at connection, `status_offline` as Last Will and at `stop()` (QoS 1) |
| `status_online` / `status_offline` | `"online"` / `"offline"` | the two payloads |
| `will` | `None` | custom Last Will instead: `(topic, message[, retain[, qos]])` |

**Messages / supervision**

| Option | Default | Meaning |
|---|---|---|
| `queue_size` | `20` | messages kept in RAM while offline (the oldest are dropped) |
| `reply_suffix` | `"/reply"` | added to the topic for `reply=True` |
| `poll_ms` | `100` | how often incoming messages are read while connected |
| `backoff_base` / `backoff_max` | `2` / `120` | wait between failed attempts: 2, 4, 8 … s, capped, ±20 % jitter |

**Logging**

| Option | Default | Meaning |
|---|---|---|
| `log_level` | `"INFO"` | `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"` |
| `log_file` / `log_max_size` | `None` / `8192` | e.g. `"/mqtt.log"`, rotating like the WiFi log |
| `log_buffer` | `60` | lines kept in RAM (`mq.log.tail()`) |
| `share_log` | `True` | use the WiFi manager's RAM buffer: MQTT lines appear in its config page |

`mq.log.console = False` silences `print()`. Passwords are never logged.

**Callbacks** (plain functions or `async def`): `on_connect()`, `on_disconnect(reason)`, `on_message(topic, payload)`,
`on_delivered(mid)`, `on_state(old, new)`.

## 7. API

| Call | Description |
|---|---|
| `await mq.start(wait=True)` | asyncio: start supervision (and the WiFi manager's); with `wait` returns after the first attempt (`True` if connected) |
| `await mq.wait_connected(timeout=None)` | asyncio: wait until connected |
| `await mq.wait_delivered(mid=None, timeout=None)` | asyncio: wait for one message, or the whole queue |
| `mq.connect(timeout=None)` | blocking: WiFi + broker. `True` when connected, `False` after `timeout` s |
| `mq.check()` | blocking style: one supervision step (also `wm.check()`), returns the state. Call it every loop |
| `mq.flush(timeout=None)` | blocking: until the queue is empty |
| `mq.publish(topic, payload, qos=0, retain=False)` | send or queue; returns the message id |
| `mq.subscribe(filter, handler=None, qos=0, reply=None)` / `@mq.on(filter, qos=0, reply=None)` | subscribe (kept across reconnections) |
| `mq.unsubscribe(filter)` | remove a subscription |
| `mq.reconnect()` | drop the connection and connect again now |
| `mq.stop(disconnect=True)` | stop supervising; publishes `offline` and sends DISCONNECT (no Last Will) |
| `mq.state` | `"idle"`, `"no_wifi"`, `"connecting"`, `"connected"`, `"backoff"` |
| `mq.is_connected()`, `mq.pending()`, `mq.is_pending(mid)`, `mq.subscriptions()`, `mq.status()` | live information |
| `mq.wifi`, `mq.log` | the WiFi manager, the logger (`mq.log.info("...")` for your own lines) |
| `topic_matches(filter, topic)` | the MQTT wildcard matcher used internally |

## 8. Design notes

* **Same shape as `wifimanager`:** one class, keyword-only configuration, one supervisor step function returning "wait N ms" and three drivers
  (`start()`, `connect()`, `check()`), so blocking and asyncio users get identical behaviour.
* **`wifimanager` is a real dependency, not a copy:** `mqttmanager` uses its compatibility layer (`compat`), its logger, and its
  `is_connected()` / `start()` / `connect()` / `check()`.
* **`umqtt.simple` does the protocol** (it is in the firmware already). `mqttmanager/client.py` subclasses it and replaces only `connect()`
  (timeouts and TLS identical on every version). It adds a socket proxy that keeps a timeout on every read, drains packets umqtt.simple
  leaves unread, wraps the packet id at 65535, and provides `unsubscribe()` on versions that lack it.
* Handlers run **outside** umqtt.simple's packet parsing (messages are collected first, then dispatched), so a handler can publish,
  subscribe, or even call `reconnect()` safely.

## 9. Troubleshooting

| Symptom | Hint |
|---|---|
| state stays `no_wifi` | the WiFi side is not connected: see the `wifi:` lines in the log / the WiFi config page |
| `cannot connect: ... ECONNREFUSED` | nothing listens on that host/port: broker running? port 1883 vs 8883 (TLS)? |
| `cannot connect: OSError: -2` (or `-202`) | host name not resolved: use the IP, or check the router's DNS |
| `broker refused the connection: bad user name or password` / `not authorized` | credentials or ACLs on the broker |
| `broker refused the connection: client id rejected` / constant reconnects | two devices with the same `client_id` kick each other out |
| `broker refused the subscription to ...` | the broker's ACL forbids that topic |
| `no answer to ping` | the broker or the network path stopped answering; it reconnects by itself |
| `queue full: dropped message ...` | offline for long: raise `queue_size` (RAM!) or publish less often |
| TLS errors | pass `ssl_params={"server_hostname": ..., "cadata": ...}` or an `ssl.SSLContext`; check the clock for certificate dates (NTP) |

Use `log_level="DEBUG"` to see every state change, ping and delivered message.

## 10. Development and tests (host, no board needed)

```bash
git submodule update --init                     # deps/ESP32-wifi: the dependency, and its fake radio for the tests
python3 -m unittest discover -s tests           # supervisor on a virtual clock + fake broker; asyncio on a real loop
```

`test_legacy.py` reruns the whole suite against umqtt.simple 1.3.4 (MicroPython ≤ 1.22), 1.4.0 (1.23–1.24) and 1.6.0 (1.25–1.26). `tests/vendor/` holds
unchanged copies of each version (MIT, micropython-lib). Run against one version: `UMQTT=1.3.4 python3 -m unittest discover -s tests`.
`tests/fakes/broker.py` is an in-memory MQTT broker with knobs for crashes, stalls, refusals and missing PUBACKs.

`tests/mp_smoke.py` also runs on the **real MicroPython** (unix port):

```bash
MICROPYPATH=.frozen:tests/vendor/umqtt-1.8.1:tests/fakes:deps/ESP32-wifi/tests/fakes:deps/ESP32-wifi:. micropython tests/mp_smoke.py
```

### On-device checklist (needs a board and a broker)

1. Copy both libraries (see §1) and `examples/main_commands.py` as `/main.py`; set your WiFi and broker.
2. `mosquitto_sub -v -t 'devices/kitchen/#'`: `status online` appears; `mosquitto_pub -t devices/kitchen/cmd/led -m on` gets a reply.
3. Stop the broker: the log shows `connection lost`, then retries with backoff; start it again: it reconnects and resubscribes.
4. Switch the router off: `no_wifi`; on again: WiFi and MQTT come back by themselves. Pull the power: the broker publishes `offline`.

## 11. License

MIT — use it freely in your projects. The vendored `umqtt.simple` copies in `tests/vendor/` are MIT too (micropython-lib).
