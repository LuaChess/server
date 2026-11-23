#!/usr/bin/env python3
"""
LuaChess test server (standard library only, Python 3.8+).

One port does two things:
  * HTTP       -> serves the HTML file unchanged
  * WebSocket  -> the "server" the page's Servers tab connects to.
                  The page sends the code "luachess_server" and only treats the
                  server as online if it gets the same code back.

Usage:
    python server.py                      # finds index.html (or the only .html) next to this script
    python server.py --html game.html
    python server.py --port 9000
    python server.py --cert cert.pem --key key.pem    # HTTPS + wss:// (see below)

By default it listens on all interfaces, IPv4 and IPv6, so "localhost" and your
LAN IP both work. If a device still can't connect, check your OS firewall.

If you open the page from an https site (e.g. a hosted copy), the page uses wss://
for any host that isn't localhost, and browsers block ws:// from https pages.
For that case run with --cert/--key. A quick self-signed cert:
    openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 30 -subj "/CN=localhost"
then open https://<host>:<port> once in that browser and accept the warning so the
wss:// connection is trusted too.

Then open http://localhost:8765, go to Servers, add host "localhost" port 8765
(use your LAN IP instead of localhost to test from another device), and Connect.

Lobbies ("Play a Friend" -> Create Lobby / Join Lobby), JSON text messages:
  client -> server   {"type":"create_lobby"}
                     {"type":"join_lobby","code":"12345"}
                     {"type":"leave_lobby"}
  server -> client   {"type":"lobby_created","code":"12345"}      (to the host)
                     {"type":"lobby_joined","code":"12345"}       (to the joiner)
                     {"type":"lobby_update","code":"12345","players":1|2}   (to everyone in the lobby)
                     {"type":"lobby_closed"}                      (host left; sent to the guest)
                     {"type":"lobby_error","message":"..."}
A lobby holds at most two players (host + one friend). Any other message from a
client that is in a lobby is logged and relayed unchanged to the other player, so
future game messages (moves, resign, ...) will pass straight through.
"""
import argparse
import asyncio
import base64
import hashlib
import json
import random
import re
import socket
import ssl
import struct
import sys
import time
from pathlib import Path

CODE = "luachess_server"
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_MESSAGE = 1 << 20  # 1 MiB

clients = set()
HTML_PATH = None
_next_id = 0


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- WebSocket --
def make_frame(opcode, payload=b""):
    n = len(payload)
    head = bytes([0x80 | opcode])
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + struct.pack(">H", n)
    else:
        head += bytes([127]) + struct.pack(">Q", n)
    return head + payload


async def read_frame(reader):
    b1, b2 = await reader.readexactly(2)
    fin = bool(b1 & 0x80)
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        (length,) = struct.unpack(">H", await reader.readexactly(2))
    elif length == 127:
        (length,) = struct.unpack(">Q", await reader.readexactly(8))
    if length > MAX_MESSAGE:
        raise ValueError("frame too large")
    mask = await reader.readexactly(4) if masked else None
    payload = await reader.readexactly(length)
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return fin, opcode, payload


class Client:
    def __init__(self, writer, cid, peer):
        self.writer = writer
        self.id = cid
        self.peer = peer
        self.verified = False
        self.lobby = None

    def send_text(self, text):
        try:
            self.writer.write(make_frame(0x1, text.encode("utf-8")))
        except Exception:
            pass  # peer is going away; its own session will clean up

    def tag(self):
        return f"#{self.id} {self.peer}"


async def websocket_session(reader, writer, headers):
    global _next_id
    key = headers.get("sec-websocket-key")
    if not key:
        writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        return
    accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    writer.write(
        (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode()
    )
    await writer.drain()

    _next_id += 1
    peer = writer.get_extra_info("peername")
    c = Client(writer, _next_id, f"{peer[0]}:{peer[1]}" if peer else "?")
    clients.add(c)
    log(f"connect    {c.tag()}  (origin: {headers.get('origin', 'none')})")

    buf = b""
    buf_op = None
    try:
        while True:
            fin, op, payload = await read_frame(reader)
            if op == 0x8:  # close
                writer.write(make_frame(0x8, payload[:2]))
                await writer.drain()
                break
            if op == 0x9:  # ping
                writer.write(make_frame(0xA, payload))
                continue
            if op == 0xA:  # pong
                continue
            if op in (0x1, 0x2):
                buf_op, buf = op, payload
            elif op == 0x0 and buf_op is not None:
                buf += payload
                if len(buf) > MAX_MESSAGE:
                    raise ValueError("message too large")
            else:
                continue
            if not fin:
                continue
            if buf_op == 0x1:
                handle_text(c, buf.decode("utf-8", "replace"))
            buf, buf_op = b"", None
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, ValueError) as e:
        if not isinstance(e, asyncio.IncompleteReadError):
            log(f"error      {c.tag()}: {e}")
    finally:
        leave_lobby(c)
        clients.discard(c)
        log(f"disconnect {c.tag()}")


# ------------------------------------------------------------------ lobbies --
lobbies = {}  # code -> Lobby


class Lobby:
    def __init__(self, code, host):
        self.code = code
        self.host = host
        self.guest = None

    def players(self):
        return 1 + (self.guest is not None)

    def peer_of(self, c):
        return self.guest if c is self.host else self.host


def send_json(c, obj):
    c.send_text(json.dumps(obj, separators=(",", ":")))


def new_code():
    for _ in range(1000):
        code = f"{random.randrange(100000):05d}"
        if code not in lobbies:
            return code
    return None


def leave_lobby(c):
    lob = c.lobby
    if not lob:
        return
    c.lobby = None
    if c is lob.host:
        lobbies.pop(lob.code, None)
        if lob.guest:
            lob.guest.lobby = None
            send_json(lob.guest, {"type": "lobby_closed"})
        log(f"lobby {lob.code} closed (host {c.tag()} left)")
    else:
        lob.guest = None
        send_json(lob.host, {"type": "lobby_update", "code": lob.code, "players": 1})
        log(f"lobby {lob.code}: guest {c.tag()} left")


def lobby_error(c, message):
    send_json(c, {"type": "lobby_error", "message": message[:60]})
    log(f"lobby error -> {c.tag()}: {message}")


def create_lobby(c):
    leave_lobby(c)
    code = new_code()
    if not code:
        lobby_error(c, "No free lobby codes")
        return
    lob = Lobby(code, c)
    lobbies[code] = lob
    c.lobby = lob
    send_json(c, {"type": "lobby_created", "code": code})
    log(f"lobby {code} created by {c.tag()}")


def join_lobby(c, code):
    code = str(code or "").strip()
    if not re.fullmatch(r"\d{5}", code):
        lobby_error(c, "Enter a valid 5 digit code")
        return
    lob = lobbies.get(code)
    if not lob:
        lobby_error(c, "Lobby not found")
        return
    if lob.host is c or c.lobby is lob:
        lobby_error(c, "You are already in that lobby")
        return
    if lob.guest is not None:
        lobby_error(c, "Lobby is full")
        return
    leave_lobby(c)  # drop any lobby this client was in before
    lob.guest = c
    c.lobby = lob
    send_json(c, {"type": "lobby_joined", "code": code})
    for m in (lob.host, lob.guest):
        send_json(m, {"type": "lobby_update", "code": code, "players": 2})
    log(f"lobby {code}: {c.tag()} joined (2/2)")


def handle_text(c, text):
    if text.strip() == CODE:
        c.verified = True
        c.send_text(CODE)  # this reply is what makes the page show "Connected"
        log(f"handshake  {c.tag()} verified")
        return
    if not c.verified:
        log(f"ignored    {c.tag()} (not verified): {text[:200]!r}")
        return
    try:
        msg = json.loads(text)
    except ValueError:
        msg = None
    if not isinstance(msg, dict):
        log(f"ignored    {c.tag()} (not a JSON object): {text[:200]!r}")
        return

    t = msg.get("type")
    if t == "create_lobby":
        create_lobby(c)
    elif t == "join_lobby":
        join_lobby(c, msg.get("code"))
    elif t == "leave_lobby":
        leave_lobby(c)
    elif c.lobby and c.lobby.peer_of(c):
        peer = c.lobby.peer_of(c)
        log(f"relay      {c.tag()} -> {peer.tag()}: {text[:300]!r}")
        peer.send_text(text)
    else:
        log(f"ignored    {c.tag()} (not in a lobby with a friend): {text[:200]!r}")


# --------------------------------------------------------------------- HTTP --
def http_response(status, body=b"", ctype="text/plain; charset=utf-8"):
    reason = {200: "OK", 204: "No Content", 404: "Not Found", 405: "Method Not Allowed"}.get(status, "")
    head = (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: {ctype}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Cache-Control: no-store\r\n"
        "Connection: close\r\n\r\n"
    )
    return head.encode() + body


async def handle(reader, writer):
    try:
        data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
        lines = data.decode("latin-1").split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        if headers.get("upgrade", "").lower() == "websocket":
            await websocket_session(reader, writer, headers)
            return

        path = target.split("?", 1)[0]
        if method not in ("GET", "HEAD"):
            writer.write(http_response(405))
        elif path in ("/", "/index.html"):
            body = HTML_PATH.read_bytes()  # re-read each time so edits show up on refresh
            writer.write(http_response(200, b"" if method == "HEAD" else body, "text/html; charset=utf-8"))
        elif path == "/favicon.ico":
            writer.write(http_response(204))
        else:
            writer.write(http_response(404, b"Not found"))
        await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, ValueError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


def find_html(arg):
    if arg:
        p = Path(arg).expanduser().resolve()
        if not p.is_file():
            sys.exit(f"HTML file not found: {p}")
        return p
    here = Path(__file__).resolve().parent
    idx = here / "index.html"
    if idx.is_file():
        return idx
    found = sorted(here.glob("*.html"))
    if len(found) == 1:
        return found[0]
    sys.exit("Couldn't pick an HTML file automatically. Pass one with --html path/to/file.html")


def lan_ips():
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    try:
        u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        u.connect(("10.255.255.255", 1))
        ips.add(u.getsockname()[0])
        u.close()
    except OSError:
        pass
    return sorted(i for i in ips if not i.startswith("127."))


async def main():
    global HTML_PATH
    ap = argparse.ArgumentParser(description="LuaChess test server")
    ap.add_argument("--host", default=None, help="bind address (default: all interfaces, IPv4 + IPv6)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--html", help="path to the HTML file to serve")
    ap.add_argument("--cert", help="TLS certificate (enables https + wss)")
    ap.add_argument("--key", help="TLS private key (used with --cert)")
    args = ap.parse_args()
    HTML_PATH = find_html(args.html)

    ctx = None
    if args.cert:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.cert, args.key)
    http, wsch = ("https", "wss") if ctx else ("http", "ws")

    server = await asyncio.start_server(handle, args.host, args.port, ssl=ctx)
    log(f"serving {HTML_PATH.name}  ({wsch}:// enabled)")
    log(f"open    {http}://localhost:{args.port}")
    for ip in lan_ips():
        log(f"        {http}://{ip}:{args.port}   (other devices on your network)")
    log(f"in the page: Servers -> host 'localhost' (or the IP above), port {args.port} -> Connect")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nbye")
