"""MQTTManager: broker connection supervision, offline queue, delivery tracking, commands/replies.

The whole supervision logic is ONE step function (`_step`) that returns "wait this many
milliseconds". Three thin drivers run it, exactly like WiFiManager:

* asyncio  : `await mq.start()`   (background task; also starts the WiFiManager)
* blocking : `mq.connect()`       (returns once the broker accepted the connection)
* polling  : `mq.check()` in your own while-loop (also runs `wm.check()`)

The MQTT packets themselves are handled by umqtt.simple (see client.py).
"""
import json
import random
from binascii import hexlify

import machine
from umqtt.simple import MQTTException
from wifimanager import compat as C
from wifimanager.compat import ticks_add, ticks_diff
from wifimanager.log import Logger

from .client import Client, connack_text

__version__ = "1.0.0"

# supervisor states (what `mq.state` returns)
IDLE = "idle"
NO_WIFI = "no_wifi"
CONNECTING = "connecting"
CONNECTED = "connected"
BACKOFF = "backoff"

_MAX_BURST = 10     # incoming packets processed per step
_DROPPED_KEEP = 16  # ids of dropped messages remembered for wait_delivered()

# subscription record: [filter, qos, handler, reply, subscribed-in-this-session]
_F, _Q, _H, _R, _DONE = 0, 1, 2, 3, 4


def topic_matches(flt, topic):
    """True when `topic` matches the subscription filter `flt` ('+' = one level, '#' = the rest)."""
    if flt == topic:
        return True
    if topic[:1] == "$" and flt[:1] in ("+", "#"):
        return False  # $SYS/... is not matched by wildcards at the first level
    f = flt.split("/")
    t = topic.split("/")
    for i, part in enumerate(f):
        if part == "#":
            return True
        if i >= len(t) or (part != "+" and part != t[i]):
            return False
    return len(f) == len(t)


def _b(s):
    if s is None or isinstance(s, bytes):
        return s
    if isinstance(s, bytearray):
        return bytes(s)
    return str(s).encode()


def _payload(v):
    """str -> UTF-8, bytes as they are, None -> empty, anything else (dict, list, numbers) -> JSON."""
    if v is None:
        return b""
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, str):
        return v.encode()
    return json.dumps(v).encode()


def _check_filter(flt, qos):
    if not isinstance(flt, str) or not flt:
        raise ValueError("topic filter must be a non-empty string")
    if qos not in (0, 1):
        raise ValueError("qos must be 0 or 1 (umqtt.simple has no QoS 2)")


def _default_client_id():
    return "esp32-" + hexlify(machine.unique_id()).decode()


class MQTTManager:
    IDLE, NO_WIFI, CONNECTING, CONNECTED, BACKOFF = IDLE, NO_WIFI, CONNECTING, CONNECTED, BACKOFF

    def __init__(self, server, *, wifi=None, port=0, client_id=None, user=None, password=None,
                 keepalive=60, clean_session=True, ssl=None,
                 connect_timeout=10, response_timeout=10,
                 status_topic=None, status_online="online", status_offline="offline", will=None,
                 queue_size=20, reply_suffix="/reply", poll_ms=100,
                 backoff_base=2, backoff_max=120,
                 log_level="INFO", log_file=None, log_max_size=8192, log_buffer=60, share_log=True,
                 on_connect=None, on_disconnect=None, on_message=None, on_delivered=None,
                 on_state=None):
        if not 0 <= keepalive <= 65535:
            raise ValueError("keepalive must be 0-65535 seconds")
        if status_topic and will:
            raise ValueError("use either status_topic or will, not both")
        if will is not None and not 2 <= len(will) <= 4:
            raise ValueError("will must be (topic, message[, retain[, qos]])")
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")

        if wifi is None:
            from wifimanager import WiFiManager
            wifi = WiFiManager()
        self.wifi = wifi
        self.server = server
        self.client_id = client_id or _default_client_id()
        self.keepalive = keepalive
        self.clean_session = clean_session
        self.connect_timeout = connect_timeout
        self.response_timeout = response_timeout
        self.status_topic = status_topic
        self._online = _payload(status_online)
        self._offline = _payload(status_offline)
        self.queue_size = queue_size
        self.reply_suffix = reply_suffix
        self.poll_ms = max(10, poll_ms)
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

        self._cb_connect = on_connect
        self._cb_disconnect = on_disconnect
        self._cb_message = on_message
        self._cb_delivered = on_delivered
        self._cb_state = on_state

        self.log = Logger(log_level, log_buffer, log_file, log_max_size, tag="mqtt")
        wlog = getattr(wifi, "log", None)
        if share_log and isinstance(getattr(wlog, "_lines", None), list):
            # same RAM buffer as the WiFi log: MQTT lines show up in the config portal's log too
            self.log._lines = wlog._lines
            self.log.buffer_size = wlog.buffer_size

        c = Client(_b(self.client_id), server, port, _b(user), _b(password), keepalive,
                   ssl, response_timeout)
        c.set_callback(self._on_raw)
        if status_topic:
            will = (status_topic, status_offline, True, 1)
        if will:
            w = tuple(will) + (False, 0)[len(will) - 2:]
            _check_filter(w[0], w[3])
            c.set_last_will(_b(w[0]), _payload(w[1]), bool(w[2]), w[3])
        self._client = c
        if ssl is True:
            self.log.warning("TLS without certificate verification: pass an ssl.SSLContext with your CA to verify")

        # supervisor state
        self._sta = IDLE
        self._booted = False
        self._running = False
        self._async = False
        self._task = None
        self._epoch = 0
        self._kick = False
        self._wake = 0
        self._in_step = False
        self._first_done = False
        self._failed = 0
        self._next_try = None
        self._last_error = ""
        self._ping_at = None
        # messages and subscriptions
        self._subs = []
        self._unsubs = []
        self._outbox = []   # [mid, topic, payload, qos, retain]
        self._inbox = []    # (topic, payload) read by umqtt.simple, not yet dispatched
        self._mid = 0
        self._dropped = []
        self._n_dropped = 0

    # =====================================================================================
    # public: state
    # =====================================================================================
    @property
    def state(self):
        return self._sta

    def is_connected(self):
        return self._sta == CONNECTED

    def pending(self):
        """Number of queued messages not delivered yet."""
        return len(self._outbox)

    def is_pending(self, mid):
        for m in self._outbox:
            if m[0] == mid:
                return True
        return False

    def subscriptions(self):
        return [(s[_F], s[_Q]) for s in self._subs]

    def status(self):
        c = self._client
        return {"state": self._sta, "connected": self._sta == CONNECTED, "server": c.server,
                "port": c.port, "client_id": self.client_id, "queued": len(self._outbox),
                "dropped": self._n_dropped, "failed_attempts": self._failed,
                "last_error": self._last_error, "subscriptions": [s[_F] for s in self._subs],
                "version": __version__}

    # =====================================================================================
    # public: messages
    # =====================================================================================
    def publish(self, topic, payload, qos=0, retain=False):
        """Queue a message and send it now if connected. Returns its id (see is_pending()).

        payload: str, bytes, None (empty) or anything json.dumps() accepts (dict, list, numbers).
        While offline the message waits in RAM (at most `queue_size`, the oldest are dropped).
        """
        if not isinstance(topic, str) or not topic or "+" in topic or "#" in topic:
            raise ValueError("invalid topic for publish: %r" % (topic,))
        if qos not in (0, 1):
            raise ValueError("qos must be 0 or 1 (umqtt.simple has no QoS 2)")
        self._mid += 1
        mid = self._mid
        if len(self._outbox) >= self.queue_size:
            old = self._outbox.pop(0)
            self._forget(old[0])
            self.log.warning("queue full (%d): dropped message %d to '%s'",
                             self.queue_size, old[0], old[1].decode())
        self._outbox.append([mid, topic.encode(), _payload(payload), qos, bool(retain)])
        self._poke()
        return mid

    def subscribe(self, topic, handler=None, qos=0, reply=None):
        """Subscribe now and after every reconnection.

        handler(topic, payload) may be a plain or an `async def` function; messages without a
        handler go to on_message. With reply=True what the handler returns is published to
        topic + reply_suffix; reply="some/topic" publishes it there instead.
        """
        _check_filter(topic, qos)
        for s in self._subs:
            if s[_F] == topic:
                if s[_Q] != qos:
                    s[_DONE] = False
                s[_Q], s[_H], s[_R] = qos, handler, reply
                break
        else:
            self._subs.append([topic, qos, handler, reply, False])
        if topic in self._unsubs:
            self._unsubs.remove(topic)
        self._poke()

    def on(self, topic, qos=0, reply=None):
        """Decorator form of subscribe(): @mq.on("home/led", reply=True)."""
        def deco(fn):
            self.subscribe(topic, fn, qos, reply)
            return fn
        return deco

    def unsubscribe(self, topic):
        for i, s in enumerate(self._subs):
            if s[_F] == topic:
                del self._subs[i]
                if topic not in self._unsubs:
                    self._unsubs.append(topic)
                self._poke()
                return True
        return False

    # =====================================================================================
    # public: control
    # =====================================================================================
    def reconnect(self):
        """Drop the broker connection and connect again now."""
        if self._sta == CONNECTED:
            self._drop("reconnect requested")
        self._next_try = None
        self._failed = 0
        self._kick = True

    def stop(self, disconnect=True):
        """Stop supervising. With disconnect=True the broker gets a clean DISCONNECT (no will)."""
        if self._sta == CONNECTED and disconnect:
            try:
                if self.status_topic:
                    self._client.publish(_b(self.status_topic), self._offline, True, 1)
                self._client.disconnect()
            except Exception as e:
                self.log.warning("disconnect failed: %s", C.exc_text(e))
        self._client.close()
        self._running = False
        self._booted = False
        self._task = None
        self._epoch += 1
        self._kick = True
        self._set_sta(IDLE)
        self.log.info("stopped")

    # =====================================================================================
    # driver: asyncio
    # =====================================================================================
    async def start(self, wait=True):
        """Start background supervision (and the WiFiManager's, if not running yet).

        With wait=True returns after the first broker connection attempt: True if connected.
        """
        if self._task is not None:
            return self.is_connected()
        self._async = True
        self._boot()
        wifi_up = True
        start = getattr(self.wifi, "start", None)
        if start is not None:
            wifi_up = await start(wait=wait)
        self._task = C.create_task(self._run_async(self._epoch))
        await C.sleep_ms(0)  # let the task take its first step (state reflects reality on return)
        if wait and wifi_up:
            while self._running and not self._first_done:
                await C.sleep_ms(50)
        return self.is_connected()

    async def wait_connected(self, timeout=None):
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        while self._sta != CONNECTED:
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                return False
            await C.sleep_ms(100)
        return True

    async def wait_delivered(self, mid=None, timeout=None):
        """Wait until message `mid` (default: every queued message) is delivered.

        False on timeout or when the message was dropped because the queue was full.
        """
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        while self.is_pending(mid) if mid is not None else self._outbox:
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                return False
            await C.sleep_ms(50)
        return mid is None or mid not in self._dropped

    async def _run_async(self, epoch):
        while self._running and epoch == self._epoch:
            ms = self._step()
            end = ticks_add(C.ticks_ms(), ms)
            await C.sleep_ms(0)
            while self._running and not self._kick:
                rem = ticks_diff(end, C.ticks_ms())
                if rem <= 0:
                    break
                await C.sleep_ms(min(rem, 50))
            self._kick = False

    # =====================================================================================
    # driver: blocking / polling
    # =====================================================================================
    def connect(self, timeout=None):
        """Blocking: bring up WiFi (wm.connect()) and the broker connection.

        Returns True once connected, False when `timeout` seconds elapsed.
        """
        if self._async:
            return self.is_connected()
        self._boot()
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        wconnect = getattr(self.wifi, "connect", None)
        if wconnect is not None and not self.wifi.is_connected():
            left = None if end is None else max(1, ticks_diff(end, C.ticks_ms())) / 1000
            if not wconnect(left):
                return False
        while self._running:
            self._wifi_check()
            ms = self._step()
            if self._sta == CONNECTED:
                self._wake = ticks_add(C.ticks_ms(), ms)
                return True
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                self._wake = ticks_add(C.ticks_ms(), ms)
                return False
            self._sleep_sync(ms)
        return False

    def check(self):
        """One supervision step for your own loop (runs wm.check() too). Returns `mq.state`.

        Cheap when nothing is due: call it often (every loop iteration).
        """
        if self._async:
            return self._sta
        self._boot()
        self._wifi_check()
        if self._kick or ticks_diff(C.ticks_ms(), self._wake) >= 0:
            self._kick = False
            ms = self._step()
            self._wake = ticks_add(C.ticks_ms(), ms)
        return self._sta

    def flush(self, timeout=None):
        """Blocking: run until every queued message is delivered. False after `timeout` seconds.

        (asyncio code uses `await mq.wait_delivered()` instead.)
        """
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        while self._outbox and not self._async:
            self.check()
            if not self._outbox:
                break
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                return False
            C.sleep_ms_blocking(max(1, min(ticks_diff(self._wake, C.ticks_ms()), 100)))
        return not self._outbox

    def _sleep_sync(self, ms):
        end = ticks_add(C.ticks_ms(), ms)
        while not self._kick:
            rem = ticks_diff(end, C.ticks_ms())
            if rem <= 0:
                break
            C.sleep_ms_blocking(min(rem, 100))
            self._wifi_check()
        self._kick = False

    def _wifi_check(self):
        fn = getattr(self.wifi, "check", None)
        if fn is not None:
            fn()

    # =====================================================================================
    # helpers
    # =====================================================================================
    def _boot(self):
        if self._booted:
            return
        self._booted = True
        self._running = True
        c = self._client
        self.log.info("MQTTManager %s started (broker %s:%d, client id '%s')",
                      __version__, c.server, c.port, self.client_id)

    def _fire(self, cb, *args):
        if cb is None:
            return
        try:
            r = cb(*args)
            if C.is_coroutine(r):
                if self._async:
                    C.create_task(r)
                else:
                    C.run_until_complete(r)
        except Exception as e:
            self.log.error("callback error: %s", C.exc_text(e))

    def _set_sta(self, new):
        old = self._sta
        if new != old:
            self._sta = new
            self.log.debug("state %s -> %s", old, new)
            self._fire(self._cb_state, old, new)

    def _poke(self):
        """Send what is waiting right away when called from application code while connected."""
        if self._sta == CONNECTED and not self._in_step:
            self._in_step = True
            try:
                self._flush()
            finally:
                self._in_step = False
        else:
            self._kick = True

    def _forget(self, mid):
        self._n_dropped += 1
        self._dropped.append(mid)
        if len(self._dropped) > _DROPPED_KEEP:
            self._dropped.pop(0)

    def _backoff_ms(self):
        secs = min(self.backoff_max, self.backoff_base * (2 ** min(self._failed - 1, 16)))
        return int(secs * 1000 * (80 + random.getrandbits(6) % 41) / 100)  # +-20 % jitter

    def _on_raw(self, topic, payload):
        # called by umqtt.simple while it reads; handlers run later, outside its packet parsing
        self._inbox.append((topic, payload))

    # =====================================================================================
    # the supervisor: one step, returns how many ms to wait before the next one
    # =====================================================================================
    def _step(self):
        if not self._running:
            return 1000
        if self._in_step:  # check() called from a handler or callback: the outer step carries on
            return self.poll_ms
        self._in_step = True
        try:
            if not self.wifi.is_connected():
                if self._sta == CONNECTED:
                    self._drop("WiFi lost", NO_WIFI)
                elif self._sta != NO_WIFI:
                    self.log.info("waiting for WiFi")
                    self._set_sta(NO_WIFI)
                self._next_try = None  # connect as soon as WiFi is back
                self._failed = 0
                return 500
            if self._sta == CONNECTED:
                return self._tick()
            if self._next_try is not None:
                rem = ticks_diff(self._next_try, C.ticks_ms())
                if rem > 0:
                    self._set_sta(BACKOFF)
                    return min(rem, 1000)
            self._try_connect()
            return 0 if self._sta == CONNECTED else 100
        except Exception as e:
            self.log.error("supervisor error: %s", C.exc_text(e))
            return 2000
        finally:
            self._in_step = False

    def _try_connect(self):
        c = self._client
        self._set_sta(CONNECTING)
        self.log.info("connecting to %s:%d", c.server, c.port)
        try:
            present = c.connect(self.clean_session, self.connect_timeout)
        except MQTTException as e:
            reason = "broker refused the connection: %s" % connack_text(e.args[0] if e.args else -1)
        except Exception as e:
            reason = "cannot connect: %s" % C.exc_text(e)
        else:
            self._on_connected(present)
            return
        c.close()
        self._failed += 1
        self._first_done = True
        self._last_error = reason
        delay = self._backoff_ms()
        self._next_try = ticks_add(C.ticks_ms(), delay)
        self.log.warning("%s (attempt %d, next in %ds)", reason, self._failed, delay // 1000)
        self._set_sta(BACKOFF)

    def _on_connected(self, session_present):
        c = self._client
        self._failed = 0
        self._next_try = None
        self._last_error = ""
        self._ping_at = None
        for s in self._subs:
            s[_DONE] = False
        if self.clean_session:
            self._unsubs = []  # a fresh session has nothing to unsubscribe from
        self._set_sta(CONNECTED)
        self._first_done = True
        self.log.info("connected to %s:%d%s", c.server, c.port,
                      " (session resumed)" if session_present else "")
        if self.status_topic:
            try:
                c.publish(_b(self.status_topic), self._online, True, 1)
            except Exception as e:
                self._drop(C.exc_text(e))
                return
        self._flush()
        if self._sta == CONNECTED:
            self._fire(self._cb_connect)

    def _drop(self, reason, state=BACKOFF):
        """Close the socket without DISCONNECT (so the broker publishes the will) and retry soon."""
        self._client.close()
        self._ping_at = None
        self._last_error = reason
        self._next_try = ticks_add(C.ticks_ms(), 500 + random.getrandbits(10))  # ~0.5-1.5 s
        self._set_sta(state)
        self.log.warning("connection lost (%s)", reason)
        self._fire(self._cb_disconnect, reason)

    def _tick(self):
        c = self._client
        try:
            for _ in range(_MAX_BURST):
                if c.check_msg() is None:
                    break
        except Exception as e:
            self._dispatch()  # what was already read (and acknowledged) is still delivered
            self._drop(C.exc_text(e))
            return 0
        self._dispatch()
        if self._sta != CONNECTED:  # a handler called stop() or reconnect()
            return 0
        self._flush()
        if self._sta == CONNECTED:
            self._keepalive()
        return self.poll_ms if self._sta == CONNECTED else 0

    def _flush(self):
        """Send what is waiting: unsubscriptions, subscriptions, queued messages."""
        c = self._client
        try:
            while self._unsubs:
                t = self._unsubs[0]
                c.unsubscribe(t.encode())
                self._unsubs.pop(0)
                self.log.info("unsubscribed from '%s'", t)
            for s in self._subs:
                if s[_DONE]:
                    continue
                try:
                    c.subscribe(s[_F].encode(), s[_Q])
                    self.log.info("subscribed to '%s' (qos %d)", s[_F], s[_Q])
                except MQTTException:
                    self.log.error("broker refused the subscription to '%s'", s[_F])
                s[_DONE] = True
            while self._outbox and self._sta == CONNECTED:
                m = self._outbox[0]
                c.publish(m[1], m[2], m[4], m[3])  # QoS 1: returns once the PUBACK is in
                self._outbox.pop(0)
                self.log.debug("message %d delivered to '%s' (qos %d)", m[0], m[1].decode(), m[3])
                self._fire(self._cb_delivered, m[0])
        except Exception as e:
            self._drop(C.exc_text(e))

    def _keepalive(self):
        if not self.keepalive:
            return
        s = self._client.sock
        now = C.ticks_ms()
        if self._ping_at is not None:
            if ticks_diff(s.last_rx, self._ping_at) > 0:
                self._ping_at = None  # anything received after the ping proves the link works
            elif ticks_diff(now, self._ping_at) >= self.response_timeout * 1000:
                self._drop("no answer to ping in %ds" % self.response_timeout)
                return
        if self._ping_at is None and ticks_diff(now, s.last_tx) >= self.keepalive * 500:
            try:
                self._client.ping()
            except Exception as e:
                self._drop(C.exc_text(e))
                return
            self._ping_at = C.ticks_ms()
            self.log.debug("ping")

    # ---- incoming messages ------------------------------------------------------------------
    def _dispatch(self):
        while self._inbox:
            topic, payload = self._inbox.pop(0)
            try:
                topic = topic.decode()
            except Exception:
                topic = str(topic)
            handled = False
            for s in list(self._subs):  # handlers may (un)subscribe
                if s[_H] is None or not topic_matches(s[_F], topic):
                    continue
                r = s[_R]
                if r is True and topic.endswith(self.reply_suffix) or r == topic:
                    continue  # our own reply coming back through a '#' filter: never answer it
                handled = True
                self._call(s, topic, payload)
            if not handled:
                self._fire(self._cb_message, topic, payload)

    def _call(self, s, topic, payload):
        try:
            r = s[_H](topic, payload)
            if C.is_coroutine(r):
                if self._async:
                    C.create_task(self._await_handler(r, s, topic))
                    return
                r = C.run_until_complete(r)
        except Exception as e:
            r = self._handler_error(topic, e)
        self._reply(s, topic, r)

    async def _await_handler(self, coro, s, topic):
        try:
            r = await coro
        except Exception as e:
            r = self._handler_error(topic, e)
        self._reply(s, topic, r)

    def _handler_error(self, topic, e):
        self.log.error("handler for '%s' failed: %s", topic, C.exc_text(e))
        return {"error": C.exc_text(e)}

    def _reply(self, s, topic, result):
        to = s[_R]
        if not to or result is None:
            return
        if to is True:
            to = topic + self.reply_suffix
        self.publish(to, result, s[_Q])
