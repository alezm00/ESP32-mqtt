"""umqtt.simple (1.8.0, frozen in the MicroPython v1.29 ESP32 firmware), made safe for a long-running supervisor.

umqtt.simple does all the protocol work, connect() included. This module only adds what a device
that must stay online for months needs:

* a socket proxy (`_Sock`) keeps a timeout on every blocking read (umqtt.simple's wait_msg() calls
  setblocking(True), which drops it; TLS sockets have no settimeout(), so it is set on the TCP
  socket underneath), never returns short reads, and records the last byte received/sent;
* packets that umqtt.simple reports without reading them are drained, so the stream never gets
  out of sync, and the packet id wraps at 65535 instead of overflowing.
"""
import ssl

from umqtt.simple import MQTTClient, MQTTException
from wifimanager import compat as C

_ETIMEDOUT = 110

_CONNACK = {1: "unacceptable protocol version", 2: "client id rejected", 3: "server unavailable",
            4: "bad user name or password", 5: "not authorized"}


def connack_text(code):
    return _CONNACK.get(code, "invalid CONNACK (%s)" % code)


def tls_context(opt):
    """ssl=True -> encrypted but unverified; an ssl.SSLContext is used as it is."""
    if opt is not True:
        return opt
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class _TLS:
    """Given to umqtt.simple as its SSLContext: wraps with the real one and remembers the TCP socket."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.raw = None

    def wrap_socket(self, sock, server_hostname=None):
        self.raw = sock
        return self.ctx.wrap_socket(sock, server_hostname=server_hostname)


class _Sock:
    """The broker socket as umqtt.simple sees it after connect()."""

    def __init__(self, sock, raw, timeout):
        self.sock = sock      # what we read/write: the TCP socket or the TLS socket on top of it
        self.raw = raw        # the TCP socket, which holds the timeout
        self.timeout = timeout
        self.blocking = True
        self.last_rx = self.last_tx = C.ticks_ms()
        raw.settimeout(timeout)

    def setblocking(self, flag):
        self.blocking = flag
        self.sock.setblocking(flag)  # a TLS socket passes it down to the TCP one
        if flag:
            self.raw.settimeout(self.timeout)

    def read(self, n):
        if not self.blocking:
            data = self.sock.read(n)
            if data == b"":
                raise OSError("connection closed by the broker")
            if data:
                self.last_rx = C.ticks_ms()
            return data
        if not n:
            return b""
        data = b""
        while len(data) < n:
            part = self.sock.read(n - len(data))
            if part is None:
                raise OSError(_ETIMEDOUT)
            if not part:
                raise OSError("connection closed by the broker")
            data = part if not data else data + part
        self.last_rx = C.ticks_ms()
        return data

    def write(self, buf, n=None):
        mv = memoryview(buf)
        if n is not None:
            mv = mv[:n]
        total = len(mv)
        while len(mv):
            w = self.sock.write(mv)
            if not w:
                raise OSError("socket write failed")
            mv = mv[w:]
        self.last_tx = C.ticks_ms()
        return total

    def close(self):
        for s in (self.sock, self.raw):
            try:
                s.close()
            except Exception:
                pass


class Client(MQTTClient):
    """umqtt.simple.MQTTClient plus the fixes above. Strings must be given as bytes."""

    def __init__(self, client_id, server, port=0, user=None, password=None, keepalive=0,
                 ssl=None, response_timeout=10):
        self.tls = _TLS(tls_context(ssl)) if ssl else None
        if user and password is None:
            password = b""  # umqtt.simple sends a password whenever there is a user
        MQTTClient.__init__(self, client_id, server, port, user, password, keepalive, self.tls)
        self.response_timeout = response_timeout

    def connect(self, clean_session=True, timeout=None):
        """TCP (+TLS) connection and CONNECT/CONNACK. Returns the session-present flag."""
        self.close()
        try:
            present = MQTTClient.connect(self, clean_session, timeout)
        except (AssertionError, IndexError):  # umqtt.simple 1.8.0 on a garbled or short CONNACK
            raise MQTTException(-1)
        raw = self.tls.raw if self.tls is not None else self.sock
        self.sock = _Sock(self.sock, raw, self.response_timeout)
        return present

    def close(self):
        s = self.sock
        self.sock = None
        if s is not None:
            try:
                s.close()
            except Exception:
                pass

    def disconnect(self):
        try:
            self.sock.write(b"\xe0\0")
        finally:
            self.close()

    # ---- packets -----------------------------------------------------------------------------
    def _wrap_pid(self):
        if self.pid >= 65535:  # umqtt.simple would send packet id 65536 (0 on the wire)
            self.pid = 0

    def publish(self, topic, msg, retain=False, qos=0):
        self._wrap_pid()
        MQTTClient.publish(self, topic, msg, retain, qos)

    def subscribe(self, topic, qos=0):
        self._wrap_pid()
        MQTTClient.subscribe(self, topic, qos)

    def unsubscribe(self, topic):
        self._wrap_pid()
        MQTTClient.unsubscribe(self, topic)

    def check_msg(self):
        op = MQTTClient.check_msg(self)
        self._drain(op)
        return op

    def _drain(self, op):
        # wait_msg() returns e.g. a late PUBACK or SUBACK as a bare opcode, its body still unread
        if op is not None and op & 0xF0 != 0x30:
            n = self._recv_len()
            if n:
                self.sock.read(n)
