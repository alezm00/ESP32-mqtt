"""In-memory MQTT 3.1.1 broker plus a fake `socket` module, for host tests on a virtual clock.

    broker = Broker(clock, advance)               # clock() -> ms, advance(ms) moves the clock
    mqttmanager.client.socket = broker.socket_module()

The broker parses every packet as soon as the client writes it and queues its answers for the
client's next read, so nothing ever waits in real time. A blocking read with nothing to return
advances the clock by the socket timeout and raises ETIMEDOUT, like lwIP does.
Runs on CPython and on MicroPython (tests/mp_smoke.py).
"""
ECONNRESET = 104
ETIMEDOUT = 110
ECONNREFUSED = 111
EBADF = 9


def matches(flt, topic):
    """Independent wildcard matcher (the library's own one is what the tests check)."""
    f = flt.split("/")
    t = topic.split("/")
    for i in range(len(f)):
        if f[i] == "#":
            return True
        if i >= len(t):
            return False
        if f[i] != "+" and f[i] != t[i]:
            return False
    return len(f) == len(t)


def varlen(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return out


def u16(n):
    return bytes([n >> 8, n & 0xFF])


def packet(first, body):
    return bytes([first]) + bytes(varlen(len(body))) + bytes(body)


def _str(body, i):
    n = body[i] << 8 | body[i + 1]
    return bytes(body[i + 2:i + 2 + n]), i + 2 + n


class Msg:
    def __init__(self, topic, payload, qos, retain, client_id=None, dup=False):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.client_id = client_id
        self.dup = dup

    def __repr__(self):
        return "Msg(%r, %r, qos=%d, retain=%r)" % (self.topic, self.payload, self.qos, self.retain)


class Conn:
    """One client connection, as the broker sees it."""

    def __init__(self, broker):
        self.broker = broker
        self.inbuf = b""        # client -> broker, not parsed yet
        self.out = b""          # broker -> client, not read yet
        self.open = True
        self.dead = None        # None, "reset" or "eof"
        self.graceful = False   # DISCONNECT received
        self.connect = None     # CONNECT fields (dict)
        self.subs = {}          # filter -> granted qos
        self.pid = 0
        self.acked = []         # packet ids the client acknowledged (PUBACK)
        self.willed = False


class FakeSocket:
    def __init__(self, broker):
        self.b = broker
        self.conn = None
        self.timeout = None
        self.blocking = True
        self.closed = False

    def settimeout(self, t):
        self.timeout = t
        self.blocking = t != 0

    def setblocking(self, flag):
        self.blocking = bool(flag)
        self.timeout = None if flag else 0

    def connect(self, addr):
        b = self.b
        b.connect_attempts.append(b.clock())
        if b.down:
            raise OSError(ECONNREFUSED)
        self.conn = Conn(b)
        b.conns.append(self.conn)

    def write(self, buf):
        c = self.conn
        if c is None or self.closed:
            raise OSError(EBADF)
        if c.dead or not c.open:
            raise OSError(ECONNRESET)
        data = bytes(buf)
        c.inbuf += data
        self.b._process(c)
        return len(data)

    def read(self, n):
        c = self.conn
        if c is None or self.closed:
            raise OSError(EBADF)
        if c.dead == "reset":
            raise OSError(ECONNRESET)
        if c.out:
            k = min(n, len(c.out), self.b.read_chunk or n)
            data = c.out[:k]
            c.out = c.out[k:]
            return data
        if c.dead == "eof" or not c.open:
            return b""
        if not self.blocking:
            return None
        if self.timeout is None:
            self.b.blocked_forever += 1  # a real socket would hang here
        else:
            self.b.advance(int(self.timeout * 1000))
        raise OSError(ETIMEDOUT)

    def close(self):
        if not self.closed:
            self.closed = True
            if self.conn is not None:
                self.b._gone(self.conn)


class _SocketModule:
    def __init__(self, broker):
        self.b = broker

    def socket(self, *args):
        return FakeSocket(self.b)

    def getaddrinfo(self, host, port, *args):
        if host not in self.b.hosts:
            raise OSError(-2)  # name resolution failed
        return [(2, 1, 0, "", ("10.0.0.2", port))]


class Broker:
    def __init__(self, clock, advance, hosts=("broker.local",)):
        self.clock = clock
        self.advance = advance
        self.hosts = hosts
        self.reset()

    def reset(self):
        self.conns = []
        self.published = []      # Msg from clients (the device)
        self.wills = []          # wills the broker published
        self.retained = {}
        self.connect_attempts = []
        self.pings = 0
        self.blocked_forever = 0
        # knobs
        self.down = False            # refuse TCP connections
        self.stalled = False         # accept bytes, never answer
        self.connack_code = 0
        self.users = None            # {"user": "password"} enables authentication
        self.refuse = ()             # filters whose SUBSCRIBE gets 0x80
        self.ack_publish = True      # send PUBACK for QoS 1 publishes
        self.answer_pings = True
        self.read_chunk = None       # return at most this many bytes per read()

    def socket_module(self):
        return _SocketModule(self)

    # ---- test helpers ------------------------------------------------------------------------
    def active(self):
        return [c for c in self.conns if c.open and not c.dead and c.connect is not None]

    @property
    def last(self):
        return self.conns[-1] if self.conns else None

    def topics(self):
        return [m.topic for m in self.published]

    def inject(self, topic, payload, qos=0, retain=False):
        """A message published by somebody else."""
        if isinstance(payload, str):
            payload = payload.encode()
        self._route(topic, payload, qos, retain)

    def kill(self, mode="reset"):
        """Every connection dies at once (broker crash, cable pulled); wills are published."""
        for c in self.conns:
            if c.open and not c.dead:
                c.dead = mode
                c.open = False
                self._will(c)

    # ---- protocol ----------------------------------------------------------------------------
    def _process(self, c):
        while True:
            buf = c.inbuf
            if len(buf) < 2:
                return
            n = shift = 0
            i = 1
            while True:
                if i >= len(buf):
                    return
                b = buf[i]
                n |= (b & 0x7F) << shift
                shift += 7
                i += 1
                if not b & 0x80:
                    break
            if len(buf) < i + n:
                return
            first, body = buf[0], buf[i:i + n]
            c.inbuf = buf[i + n:]
            if not self.stalled:
                self._handle(c, first, body)

    def _handle(self, c, first, body):
        t = first >> 4
        if t == 1:
            self._on_connect(c, body)
        elif t == 3:
            qos = (first >> 1) & 3
            topic, i = _str(body, 0)
            if qos:
                pid = body[i] << 8 | body[i + 1]
                i += 2
            payload = bytes(body[i:])
            cid = c.connect["client_id"] if c.connect else None
            self.published.append(Msg(topic.decode(), payload, qos, bool(first & 1), cid, bool(first & 8)))
            if qos == 1 and self.ack_publish:
                c.out += b"\x40\x02" + u16(pid)
            self._route(topic.decode(), payload, qos, bool(first & 1))
        elif t == 4:
            c.acked.append(body[0] << 8 | body[1])
        elif t == 8:
            pid = body[0] << 8 | body[1]
            i = 2
            codes = bytearray()
            new = []
            while i < len(body):
                f, i = _str(body, i)
                q = body[i]
                i += 1
                f = f.decode()
                if f in self.refuse:
                    codes.append(0x80)
                else:
                    c.subs[f] = q
                    codes.append(q)
                    new.append(f)
            c.out += packet(0x90, u16(pid) + bytes(codes))
            for topic in list(self.retained):
                payload, qos = self.retained[topic]
                for f in new:
                    if matches(f, topic):
                        self._send(c, topic, payload, min(qos, c.subs[f]), True)
                        break
        elif t == 10:
            pid = body[0] << 8 | body[1]
            i = 2
            while i < len(body):
                f, i = _str(body, i)
                c.subs.pop(f.decode(), None)
            c.out += b"\xb0\x02" + u16(pid)
        elif t == 12:
            self.pings += 1
            if self.answer_pings:
                c.out += b"\xd0\x00"
        elif t == 14:
            c.graceful = True
            c.open = False

    def _on_connect(self, c, body):
        n = body[0] << 8 | body[1]
        i = 2 + n
        level, flags = body[i], body[i + 1]
        keepalive = body[i + 2] << 8 | body[i + 3]
        cid, i = _str(body, i + 4)
        will = user = password = None
        if flags & 0x04:
            wt, i = _str(body, i)
            wm, i = _str(body, i)
            will = (wt.decode(), wm, bool(flags & 0x20), (flags >> 3) & 3)
        if flags & 0x80:
            user, i = _str(body, i)
        if flags & 0x40:
            password, i = _str(body, i)
        c.connect = {"client_id": cid.decode(), "level": level, "keepalive": keepalive,
                     "clean": bool(flags & 0x02), "will": will,
                     "user": user.decode() if user is not None else None,
                     "password": password.decode() if password is not None else None}
        rc = self.connack_code
        if not rc and self.users is not None:
            if self.users.get(c.connect["user"]) != c.connect["password"]:
                rc = 4
        c.out += b"\x20\x02\x00" + bytes([rc])
        if rc:
            c.open = False  # the broker closes after a refusal

    def _route(self, topic, payload, qos, retain):
        if retain:
            if payload:
                self.retained[topic] = (payload, qos)
            else:
                self.retained.pop(topic, None)
        for c in self.conns:
            if not c.open or c.dead or c.connect is None:
                continue
            best = -1
            for f in c.subs:
                if matches(f, topic) and c.subs[f] > best:
                    best = c.subs[f]
            if best >= 0:
                self._send(c, topic, payload, min(qos, best), False)

    def _send(self, c, topic, payload, qos, retain):
        body = u16(len(topic.encode())) + topic.encode()
        if qos:
            c.pid = c.pid % 65535 + 1
            body += u16(c.pid)
        c.out += packet(0x30 | qos << 1 | int(retain), body + payload)

    def _gone(self, c):
        if c.open and not c.dead:
            c.open = False
            self._will(c)

    def _will(self, c):
        if c.graceful or c.willed or not c.connect or not c.connect["will"]:
            return
        c.willed = True
        topic, payload, retain, qos = c.connect["will"]
        self.wills.append(Msg(topic, payload, qos, retain, c.connect["client_id"]))
        self._route(topic, payload, qos, retain)
