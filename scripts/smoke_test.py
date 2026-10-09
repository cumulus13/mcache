#!/usr/bin/env python3
"""Black-box smoke test for a RUNNING server (stdlib only). Speaks raw RESP2, no client library.

  python scripts/smoke_test.py [--host 127.0.0.1] [--port 6379] [--password PW]
Exit code 0 = all checks passed.
"""
import argparse, socket, sys, time

def enc(*parts):
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        b = p if isinstance(p, bytes) else str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)

class Conn:
    def __init__(self, host, port):
        self.s = socket.create_connection((host, port), timeout=5)
        self.buf = b""
    def _fill(self):
        d = self.s.recv(65536)
        if not d: raise ConnectionError("closed")
        self.buf += d
    def line(self):
        while b"\r\n" not in self.buf: self._fill()
        l, self.buf = self.buf.split(b"\r\n", 1)
        return l
    def exact(self, n):
        while len(self.buf) < n: self._fill()
        d, self.buf = self.buf[:n], self.buf[n:]
        return d
    def reply(self):
        l = self.line(); t, rest = l[:1], l[1:]
        if t in (b"+", b"-"): return (t.decode(), rest.decode())
        if t == b":": return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0: return None
            d = self.exact(n + 2)[:-2]; return d.decode()
        if t == b"*":
            n = int(rest); return [self.reply() for _ in range(n)]
        raise ValueError(l)
    def cmd(self, *a):
        self.s.sendall(enc(*a)); return self.reply()

fails = 0
def check(name, got, want):
    global fails
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got={got!r} want={want!r}"))
    if not ok: fails += 1

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int, default=6379)
ap.add_argument("--password", default="")
a = ap.parse_args()

c = Conn(a.host, a.port)
if a.password: check("AUTH", c.cmd("AUTH", a.password), ("+", "OK"))
k = "smoke:%d" % int(time.time() * 1000)

check("PING", c.cmd("PING"), ("+", "PONG"))
check("PING msg", c.cmd("PING", "hi"), "hi")
check("SET", c.cmd("SET", k, "héllo ✓"), ("+", "OK"))
check("GET utf8", c.cmd("GET", k), "héllo ✓")
check("GET missing", c.cmd("GET", k + ":none"), None)
check("SET NX on existing", c.cmd("SET", k, "x", "NX"), None)
check("INCR", c.cmd("INCR", k + ":n"), 1)
check("INCRBY", c.cmd("INCRBY", k + ":n", 41), 42)
check("INCR non-int", c.cmd("INCR", k)[0], "-")
check("APPEND", c.cmd("APPEND", k + ":a", "ab"), 2)
check("STRLEN", c.cmd("STRLEN", k + ":a"), 2)
check("EXPIRE", c.cmd("EXPIRE", k, 100), 1)
ttl = c.cmd("TTL", k); check("TTL range", 0 < ttl <= 100, True)
check("PERSIST", c.cmd("PERSIST", k), 1)
check("TTL none", c.cmd("TTL", k), -1)
check("SET PX", c.cmd("SET", k + ":px", "v", "PX", 150), ("+", "OK"))
time.sleep(0.4)
check("PX expired", c.cmd("GET", k + ":px"), None)
check("MSET", c.cmd("MSET", k + ":1", "a", k + ":2", "b"), ("+", "OK"))
check("MGET", c.cmd("MGET", k + ":1", k + ":zz", k + ":2"), ["a", None, "b"])
check("KEYS", sorted(c.cmd("KEYS", k + ":?")), [k + ":1", k + ":2"])
check("DEL", c.cmd("DEL", k + ":1", k + ":2", k + ":zz"), 2)
check("unknown cmd", c.cmd("NOSUCHCMD")[0], "-")

# pipelining: 3 commands in one write
c.s.sendall(enc("PING") + enc("SET", k + ":p", "1") + enc("INCR", k + ":p"))
check("pipeline", [c.reply(), c.reply(), c.reply()], [("+", "PONG"), ("+", "OK"), 2])

# inline (telnet style)
c.s.sendall(b"PING\r\n"); check("inline PING", c.reply(), ("+", "PONG"))

# invalid UTF-8 => protocol error + close
c2 = Conn(a.host, a.port)
if a.password: c2.cmd("AUTH", a.password)
c2.s.sendall(b"*2\r\n$3\r\nGET\r\n$2\r\n\xff\xfe\r\n")
r = c2.reply(); check("invalid utf8 -> protocol error", r[0] == "-" and "Protocol error" in r[1], True)

c.cmd("DEL", k, k + ":n", k + ":a", k + ":p")
print("\n%s (%d failed)" % ("ALL PASSED" if not fails else "FAILURES", fails))
sys.exit(1 if fails else 0)
