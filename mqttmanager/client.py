"""umqtt.simple, made safe for a long-running supervisor.

The protocol work (PUBLISH, SUBSCRIBE, incoming messages, PUBACK, PING) is left to umqtt.simple,
which is frozen in the official ESP32 firmware. This module only adds what a device that must
stay online for months needs:

* `connect()` is done here: socket timeout, TLS and CONNECT/CONNACK work the same way with every
  umqtt.simple version shipped since MicroPython 1.20 (1.3.4, 1.4.0, 1.6.0, 1.8.1: their connect()
  signatures and TLS arguments all differ);
* a socket proxy (`_Sock`) gives every blocking read a timeout (umqtt.simple's wait_msg() calls
  setblocking(True), which would otherwise remove it) and never returns short reads;
* packets that umqtt.simple reports without reading them are drained, so the stream never gets
  out of sync; the packet id wraps at 65535; unsubscribe() exists on every version.
"""
import struct

try:
    import socket
except ImportError:  # very old MicroPython
    import usocket as socket

from umqtt.simple import MQTTClient, MQTTException
from wifimanager import compat as C

_EAGAIN = 11
_ETIMEDOUT = 110

_CONNACK = {1: "unacceptable protocol version", 2: "client id rejected", 3: "server unavailable",
            4: "bad user name or password", 5: "not authorized"}


def connack_text(code):
    return _CONNACK.get(code, "invalid CONNACK (%s)" % code)


def _varlen(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if not n:
            out.append(b)
            return out
        out.append(b | 0x80)


def _wrap_tls(raw, tls, params, server):
    if tls is not True:  # an ssl.SSLContext (MicroPython >= 1.23)
        return tls.wrap_socket(raw, server_hostname=server)
    try:
        import ssl
    except ImportError:
        import ussl as ssl
    p = dict(params or {})
    p.setdefault("server_hostname", server)
    return ssl.wrap_socket(raw, **p)


class _Sock:
    """The broker socket as umqtt.simple sees it: timeouts on blocking reads, full reads/writes,
    and the time of the last byte received/sent (for keepalive)."""

    def __init__(self, raw, timeout):
        self.raw = raw        # the TCP socket (holds the timeout)
        self.sock = raw       # what we read/write: the TCP socket or the TLS socket on top of it
        self.timeout = timeout
        self.blocking = True
        self.last_rx = self.last_tx = C.ticks_ms()
        raw.settimeout(timeout)

    def setblocking(self, flag):
        self.blocking = flag
        if not flag:
            self.sock.setblocking(False)
            return
        if self.sock is not self.raw:
            self.sock.setblocking(True)
        self.raw.settimeout(self.timeout)

    def read(self, n):
        if not self.blocking:
            try:
                data = self.sock.read(n)
            except OSError as e:
                if e.args and e.args[0] == _EAGAIN:
                    return None
                raise
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
        try:
            self.sock.close()
        except Exception:
            pass
        if self.sock is not self.raw:
            try:
                self.raw.close()
            except Exception:
                pass


class Client(MQTTClient):
    """umqtt.simple.MQTTClient plus the fixes above. Strings must be given as bytes."""

    def __init__(self, client_id, server, port=0, user=None, password=None, keepalive=0,
                 ssl=False, ssl_params=None, response_timeout=10):
        port = port or (8883 if ssl else 1883)
        # positional, no TLS arguments: the only constructor form every umqtt.simple version accepts
        MQTTClient.__init__(self, client_id, server, port, user, password, keepalive)
        self.tls = ssl or None
        self.tls_params = ssl_params
        self.response_timeout = response_timeout

    # ---- connection --------------------------------------------------------------------------
    def connect(self, clean_session=True, timeout=None):
        """TCP (+TLS) connection and CONNECT/CONNACK. Returns the session-present flag."""
        self.close()
        addr = socket.getaddrinfo(self.server, self.port)[0][-1]
        s = _Sock(socket.socket(), timeout)
        self.sock = s
        s.raw.connect(addr)
        if self.tls is not None:
            s.sock = _wrap_tls(s.raw, self.tls, self.tls_params, self.server)
            s.setblocking(True)
        s.write(self._connect_packet(clean_session))
        resp = s.read(4)
        if resp[0] != 0x20 or resp[1] != 0x02:
            raise MQTTException(-1)
        if resp[3]:
            raise MQTTException(resp[3])
        s.timeout = self.response_timeout
        s.setblocking(True)
        return resp[2] & 1

    def _connect_packet(self, clean_session):
        flags = 0x02 if clean_session else 0
        fields = [self.client_id]
        if self.lw_topic:
            flags |= 0x04 | (self.lw_qos << 3) | (bool(self.lw_retain) << 5)
            fields.append(self.lw_topic)
            fields.append(self.lw_msg)
        if self.user:
            flags |= 0x80
            fields.append(self.user)
            if self.pswd is not None:
                flags |= 0x40
                fields.append(self.pswd)
        body = bytearray(b"\x00\x04MQTT\x04")
        body.append(flags)
        body += struct.pack("!H", self.keepalive)
        for f in fields:
            body += struct.pack("!H", len(f))
            body += f
        pkt = bytearray(b"\x10")
        pkt += _varlen(len(body))
        pkt += body
        return pkt

    def close(self):
        s = self.sock
        self.sock = None
        if s is not None:
            s.close()

    def disconnect(self):
        try:
            self.sock.write(b"\xe0\0")
        finally:
            self.close()

    # ---- packets -----------------------------------------------------------------------------
    def _wrap_pid(self):
        if self.pid >= 65535:  # umqtt.simple would send packet id 65536 / 0
            self.pid = 0

    def publish(self, topic, msg, retain=False, qos=0):
        self._wrap_pid()
        MQTTClient.publish(self, topic, msg, retain, qos)

    def subscribe(self, topic, qos=0):
        self._wrap_pid()
        MQTTClient.subscribe(self, topic, qos)

    def unsubscribe(self, topic):
        # umqtt.simple < 1.8 has no unsubscribe(): one implementation for every version
        self._wrap_pid()
        self.pid += 1
        pid = self.pid
        pkt = bytearray(b"\xa2")
        pkt += _varlen(4 + len(topic))
        pkt += struct.pack("!HH", pid, len(topic))
        pkt += topic
        self.sock.write(pkt)
        while True:
            op = self.wait_msg()
            if op == 0xB0:
                resp = self.sock.read(3)
                if resp[0] != 2 or (resp[1] << 8 | resp[2]) != pid:
                    raise MQTTException(-1)
                return
            self._drain(op)

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
