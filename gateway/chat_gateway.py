"""Gateway DOG/1 sobre TCP y HTTPS con bibliotecas estándar."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
from pathlib import Path
import re
import socket
import ssl
import struct
import sys
from threading import BoundedSemaphore
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
from chat_model import (HISTORY_GRANT, LINK_START, LINK_APPROVE, LINK_CANCEL, DEVICE, DEVICE_REVOKE, SECURE_ENABLE, SECURE_SEND, ADD, DIRECT, GROUP, REMOVE, REVOKE, SEND, REGISTER, SESSION,
                        ITERATIONS, Rejected, require, session_hash, verifier)
from chat_service import Chat, Consensus, chat_feed, BARRIER
from gateway.qr_image import png as qr_png
from gateway.device_proof import verify, verify_device
from e2ee_model import identity as public_identity, link_time
from gateway.media_gateway import Media, OPERATIONS as MEDIA_OPERATIONS

MAX_REQUEST = 16_384
MAX_REPLY = 2_097_152
MAX_POSITION = (1 << 63) - 1
ID = re.compile(r"[0-9a-f]{32}")
CURSOR = re.compile(r"(?:0|[1-9][0-9]{0,18})")
READ = {"users": (), "conversations": (), "groups": (), "group-members": ("conversation",),
        "history": ("conversation", "after", "limit"), "sync": ("conversation", "after", "limit"),
        "status": ("request",)}
WRITE = {"logout": (), "direct": ("peer",), "group-create": ("name", "members"),
         "group-add": ("conversation", "member"), "group-remove": ("conversation", "member"),
         "send": ("conversation", "text")}
PUBLIC = {"register": ("username", "display", "password"), "login": ("username", "password")}
STATIC = {"/e2ee.js": ("e2ee.js", "text/javascript"),"/": ("index.html", "text/html; charset=utf-8"), "/styles.css": ("styles.css", "text/css"),
          "/app.js": ("app.js", "text/javascript")}
SECURE_OPS = {
    "device-register": {"device", "exchange", "signing", "proof"}, "device-revoke": {"device"},
    "devices": set(), "e2ee-directory": {"conversation"}, "e2ee-enable": {"conversation"},
    "e2ee-send": {"conversation", "device", "epoch", "envelope"},
    "link-start": {"device", "exchange", "signing", "challenge", "issued", "proof"},
    "link-info": {"link"}, "link-qr": {"link"}, "link-approve": {"link", "device", "observed", "proof"},
    "link-cancel": {"link"},
    "history-grant": {"message", "device", "target", "envelope"},
}
POSITION_KEYS = {"position", "next_cursor"}


def unique_pairs(pairs):
    output = {}
    for key, value in pairs:
        require(key not in output, "INVALID_JSON")
        output[key] = value
    return output


def wire_positions(value):
    if isinstance(value, list):
        return [wire_positions(item) for item in value]
    if isinstance(value, dict):
        return {key: str(item) if key in POSITION_KEYS else wire_positions(item)
                for key, item in value.items()}
    return value


def position(value):
    require(isinstance(value, str) and CURSOR.fullmatch(value), "INVALID_CURSOR")
    parsed = int(value)
    require(parsed <= MAX_POSITION, "INVALID_CURSOR")
    return parsed


class Gateway:
    def __init__(self, chat, client_key, media=None, link_ttl=120):
        self.chat = chat
        self.client_key = client_key
        self.media = media
        self.link_ttl = link_ttl
        self.algorithm = "raft" if chat.consensus.coordinator is None else "2pc"

    def derive(self, label, request):
        return hmac.digest(self.client_key, label + request, "sha256")

    def dispatch(self, raw, bearer=None, web=False):
        response = {"v": 1, "id": None, "algorithm": self.algorithm}
        try:
            req = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs)
            require(isinstance(req, dict), "INVALID_REQUEST")
            rid = req.get("id")
            require(isinstance(rid, str) and ID.fullmatch(rid) and int(rid, 16), "INVALID_ID")
            response["id"] = rid
            require(type(req.get("v")) is int and req["v"] == 1, "INVALID_VERSION")
            require(set(req) <= {"v", "id", "op", "data", "session", "min_cursor"}, "INVALID_REQUEST")
            op, data = req.get("op"), req.get("data")
            require(isinstance(op, str) and isinstance(data, dict), "INVALID_REQUEST")
            require(not web or "session" not in req, "INVALID_REQUEST")
            token = bearer if web else req.get("session")
            minimum = position(req.get("min_cursor", "0"))
            if op in SECURE_OPS:
                result, cursor = self.secure_call(op, data, rid, session_hash(token), minimum)
                return {**response, "ok": True, "cursor": str(cursor), "result": wire_positions(result)}
            if op in MEDIA_OPERATIONS:
                require(self.media is not None, "MEDIA_DISABLED")
                result, cursor = self.media.execute(self, op, data, rid, session_hash(token), minimum)
                return {**response, "ok": True, "cursor": str(cursor), "result": wire_positions(result)}
            fields = (PUBLIC | READ | WRITE).get(op)
            require(fields is not None, "UNKNOWN_OPERATION")
            require(set(data) == set(fields), "INVALID_DATA")
            for field, value in data.items():
                if field == "members":
                    require(isinstance(value, list) and len(value) <= 63 and
                            all(isinstance(m, str) and ID.fullmatch(m) for m in value), "INVALID_MEMBERS")
                elif field == "limit":
                    require(type(value) is int and 1 <= value <= 100, "INVALID_LIMIT")
                else:
                    require(isinstance(value, str) and len(value.encode("utf-8")) <= 4096, "INVALID_DATA")
                    if field in {"conversation", "peer", "member", "request"}:
                        require(ID.fullmatch(value), "INVALID_ID")
            result, cursor = self.execute(op, data, bytes.fromhex(rid), token, minimum)
            return {**response, "ok": True, "cursor": str(cursor), "result": wire_positions(result)}
        except Rejected as error:
            return {**response, "ok": False, "error": str(error)}
        except (ValueError, RecursionError):
            return {**response, "ok": False, "error": "INVALID_REQUEST"}
        except OSError:
            return {**response, "ok": False, "error": "BACKEND_UNAVAILABLE"}

    def secure_call(self, op, data, rid, session, minimum):
        require(set(data) == SECURE_OPS[op], "INVALID_DATA")
        for name, value in data.items():
            require(isinstance(value, str) and value.isascii() and len(value) <= 8192, "INVALID_DATA")
            if name in {"device", "conversation", "link", "message", "target"}:
                require(ID.fullmatch(value), "INVALID_ID")
        if op in {"devices", "e2ee-directory"}:
            view = self.authorized_view(session, minimum)
            actor = view.actor(session)
            result = (view.secure_directory(session, data["conversation"]) if op == "e2ee-directory"
                      else [device for device in view.devices.values() if device["user"] == actor])
            return result, view.cursor
        if op.startswith("link-"):
            return self.link_call(op, data, rid, session, minimum)
        if op == "device-register":
            # Verifica posesión antes de asociar una sesión al dispositivo.
            require(verify_device(data, rid, session), "INVALID_DEVICE_PROOF")
            kind, fields = DEVICE, (data["device"], data["exchange"], data["signing"])
        elif op == "device-revoke":
            kind, fields = DEVICE_REVOKE, (data["device"],)
        elif op == "e2ee-enable":
            kind, fields = SECURE_ENABLE, (data["conversation"],)
        elif op == "history-grant":
            kind, fields = HISTORY_GRANT, (data["message"], data["device"], data["target"], data["envelope"])
        else:
            kind, fields = SECURE_SEND, (data["conversation"], rid, data["device"], data["epoch"], data["envelope"])
        command = self.chat.operation(kind, session, *fields, token=bytes.fromhex(rid))
        result = self.chat.submit(command)
        return result, result["position"] if result.get("application", {}).get("status") in {
            "APPLIED", "REJECTED"} else 0

    def link_call(self, op, data, rid, session, minimum):
        now = int(time.time())
        if op == "link-start":
            start = link_time(data["issued"])
            require(abs(now - start) <= 10 and now < start + self.link_ttl, "LINK_TIME_SKEW")
            device = public_identity(data["exchange"], data["signing"])
            require(device == data["device"] and re.fullmatch(r"[0-9a-f]{64}", data["challenge"]),
                    "INVALID_LINK")
            statement = "\n".join(("dog.link.start.1", rid, session, device, data["challenge"], data["issued"]))
            require(verify(data["signing"], data["proof"], statement, data["exchange"]), "INVALID_DEVICE_PROOF")
            kind = LINK_START
            fields = (device, data["exchange"], data["signing"], data["challenge"], data["issued"],
                      str(start + self.link_ttl))
        else:
            view = self.authorized_view(session, minimum)
            now = int(time.time())
            link = view.links.get(data["link"])
            require(link is not None and link["user"] == view.actor(session), "UNKNOWN_LINK")
            if op in {"link-info", "link-qr"}:
                result = {k: v for k, v in link.items() if k != "candidate"}
                if result["status"] == "PENDING" and now >= result["expires"]:
                    result["status"] = "EXPIRED"
                if op == "link-qr":
                    require(result["status"] == "PENDING", "LINK_UNAVAILABLE")
                    public = {k: result[k] for k in ("id", "device", "challenge", "expires")}
                    text = json.dumps({"v": 1, **public}, separators=(",", ":"), sort_keys=True)
                    try:
                        image = base64.b64encode(qr_png(text.encode("ascii"))).decode("ascii")
                    except (OSError, ValueError):
                        image = None
                    result = {"descriptor": text, "png": image}
                return result, view.cursor
            if op == "link-cancel":
                kind, fields = LINK_CANCEL, (link["id"],)
            else:
                retry = link["status"] == "LINKED" and link.get("approval") == rid
                require(link["status"] == "PENDING" or retry, "LINK_USED")
                stamp = link_time(data["observed"])
                issuer = data["device"]
                if retry:
                    require(issuer == link["approver"] and stamp == link["approved_at"], "LINK_USED")
                else:
                    require(link["issued"] <= stamp < link["expires"] and now < link["expires"]
                            and abs(now - stamp) <= 10, "LINK_EXPIRED")
                require(view.device_sessions.get(session) == issuer, "DEVICE_REQUIRED")
                row = view.devices[issuer]
                statement = "\n".join(("dog.link.approve.1", rid, session, issuer, link["id"], link["device"],
                                        link["challenge"], data["observed"]))
                require(verify(row["signing"], data["proof"], statement), "INVALID_DEVICE_PROOF")
                require(retry or int(time.time()) < link["expires"], "LINK_EXPIRED")
                kind, fields = LINK_APPROVE, (link["id"], issuer, data["observed"])
        command = self.chat.operation(kind, session, *fields, token=bytes.fromhex(rid))
        result = self.chat.submit(command)
        return result, result["position"] if result.get("application", {}).get("status") in {
            "APPLIED", "REJECTED"} else 0

    def authorized_view(self, digest, minimum):
        barrier = self.chat.operation(BARRIER, digest)
        raw = barrier.encode(self.chat.consensus.secret)
        consensus, committed = self.chat.consensus.submit_operation(raw, barrier.request)
        require(consensus == "COMMITTED", "AUTHORIZATION_UNAVAILABLE")
        view = self.chat.consensus.state(max(minimum, committed))
        application = view.results.get(barrier.request.hex(), {}).get("result", {})
        require(application.get("reason") != "AUTH_REQUIRED", "AUTH_REQUIRED")
        require(application.get("status") == "APPLIED", "AUTHORIZATION_UNAVAILABLE")
        return view

    def execute(self, op, data, request, token, minimum):
        if op == "register":
            salt = self.derive(b"register-salt\0", request)[:16]
            command = self.chat.operation(REGISTER, request.hex(), data["username"], data["display"],
                                          salt.hex(), str(ITERATIONS), verifier(data["password"], salt),
                                          token=request)
        elif op == "login":
            user = self.chat.consensus.state(minimum).authenticate(data["username"], data["password"])
            session = self.derive(b"login-session\0", request + bytes.fromhex(user)).hex()
            command = self.chat.operation(SESSION, user, session_hash(session), token=request)
        else:
            digest = session_hash(token)
            args = tuple(data[key] for key in (WRITE.get(op) or ()))
            if op in READ:
                minimum = max(minimum, position(data.get("after", "0")))
                view = self.authorized_view(digest, minimum)
                result = view.query(digest, "members" if op == "group-members" else op,
                                    cid=data.get("conversation"), after=position(data.get("after", "0")),
                                    limit=data.get("limit", 50), request=data.get("request"))
                if op == "conversations":
                    return {"items": result, "user": view.actor(digest)}, view.cursor
                return result, view.cursor
            if op == "send":
                args = (data["conversation"], request.hex(), data["text"])
            elif op == "group-create":
                args = (request.hex(), data["name"], ",".join(sorted(set(data["members"]))))
            kind = {"direct": DIRECT, "send": SEND, "logout": REVOKE, "group-create": GROUP,
                    "group-add": ADD, "group-remove": REMOVE}[op]
            command = self.chat.operation(kind, digest, *args, token=request)
        result = self.chat.submit(command)
        if op == "login" and result.get("application", {}).get("status") == "APPLIED":
            self.chat.consensus.state(result["position"]).actor(session_hash(session))
            result["session"] = session
        return result, result["position"] if result.get("application", {}).get("status") in {
            "APPLIED", "REJECTED"} else 0


class Input:
    def __init__(self, conn):
        self.conn = conn
        self.buffer = bytearray()
        self.deadline = time.monotonic() + 5

    def fill(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        self.conn.settimeout(remaining)
        chunk = self.conn.recv(4096)
        if not chunk:
            raise EOFError()
        self.buffer.extend(chunk)

    def take(self, size):
        while len(self.buffer) < size:
            self.fill()
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    def headers(self):
        while True:
            end = self.buffer.find(b"\r\n\r\n")
            if end >= 0:
                require(end <= 8192, "HEADERS_TOO_LARGE")
                return self.take(end + 4)[:-4].decode("ascii").split("\r\n")
            require(len(self.buffer) <= 8192, "HEADERS_TOO_LARGE")
            self.fill()


def encode_reply(reply):
    raw = json.dumps(reply, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    if len(raw) > MAX_REPLY:
        return encode_reply({"v": 1, "id": reply["id"], "algorithm": reply["algorithm"],
                             "ok": False, "error": "REPLY_TOO_LARGE"})
    return raw


def http_response(conn, status, content_type, raw):
    headers = (f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: {content_type}\r\n"
               f"Content-Length: {len(raw)}\r\nCache-Control: no-store\r\n"
               "X-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n"
               "Content-Security-Policy: default-src 'self'; img-src 'self' blob:; object-src 'none'; base-uri 'none'; "
               "frame-ancestors 'none'\r\n\r\n")
    conn.settimeout(5)
    conn.sendall(headers.encode("ascii") + raw)


def http(conn, stream, gateway, allow_origin):
    try:
        request, *lines = stream.headers()
        method, path, version = request.split(" ")
        require(version == "HTTP/1.1", "INVALID_HTTP")
        headers = {}
        for line in lines:
            name, value = line.split(":", 1)
            name = name.lower()
            require(re.fullmatch(r"[a-z0-9!#$%&'*+.^_`|~-]+", name) and name not in headers,
                    "INVALID_HTTP")
            require(all(32 <= ord(c) < 127 or c == "\t" for c in value), "INVALID_HTTP")
            headers[name] = value.strip()
        require(headers.get("host") and "transfer-encoding" not in headers
                and "expect" not in headers, "INVALID_HTTP")
        if method not in {"GET", "POST"}:
            return http_response(conn, "405 Method Not Allowed", "text/plain", b"Metodo invalido")
        if method == "GET" and path in STATIC:
            require(headers.get("content-length", "0") == "0", "INVALID_HTTP")
            filename, mime = STATIC[path]
            return http_response(conn, "200 OK", mime, (ROOT / "clients/web" / filename).read_bytes())
        if method != "POST" or path != "/api/v1":
            return http_response(conn, "404 Not Found", "text/plain", b"Ruta invalida")
        require(headers.get("content-type") == "application/json", "INVALID_CONTENT_TYPE")
        length = headers.get("content-length", "")
        require(re.fullmatch(r"[1-9][0-9]{0,4}", length), "INVALID_LENGTH")
        if int(length) > MAX_REQUEST:
            return http_response(conn, "413 Content Too Large", "text/plain", b"Solicitud demasiado grande")
        bearer = headers.get("authorization", "")
        require(not bearer or bearer.startswith("Bearer "), "AUTH_REQUIRED")
        raw = stream.take(int(length))
        if headers.get("origin") not in (None, allow_origin):
            return http_response(conn, "403 Forbidden", "text/plain", b"Origen invalido")
        response = encode_reply(gateway.dispatch(raw, bearer[7:] or None, web=True))
        http_response(conn, "200 OK", "application/json", response)
    except (ValueError, EOFError):
        http_response(conn, "400 Bad Request", "text/plain", b"Solicitud invalida")


def connection(conn, gateway, context, origin):
    with conn:
        conn.settimeout(5)
        if context:
            conn = context.wrap_socket(conn, server_side=True)
        with conn:
            stream = Input(conn)
            if origin is not None:
                http(conn, stream, gateway, origin)
            else:
                size, = struct.unpack(">I", stream.take(4))
                if not 0 < size <= MAX_REQUEST:
                    return
                raw = encode_reply(gateway.dispatch(stream.take(size)))
                conn.settimeout(5)
                conn.sendall(struct.pack(">I", len(raw)) + raw)


def serve(gateway, context, host, tcp, web, origin, workers):
    slots = BoundedSemaphore(workers)

    def handle(conn, origin):
        try:
            connection(conn, gateway, context, origin)
        except (OSError, EOFError):
            pass
        finally:
            conn.close()
            slots.release()

    import selectors
    from contextlib import ExitStack
    with ExitStack() as stack:
        executor = stack.enter_context(ThreadPoolExecutor(max_workers=workers))
        selector = stack.enter_context(selectors.DefaultSelector())
        for port, endpoint in ((tcp, None), (web, origin)):
            listener = stack.enter_context(socket.socket())
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen(workers)
            selector.register(listener, selectors.EVENT_READ, endpoint)
            print(f"Gateway {gateway.algorithm} {host}:{listener.getsockname()[1]}", flush=True)
        while True:
            for key, _ in selector.select():
                conn, _ = key.fileobj.accept()
                if slots.acquire(blocking=False):
                    executor.submit(handle, conn, key.data)
                else:
                    conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tcp", type=int, default=9443)
    parser.add_argument("--web", type=int, default=8443)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--cert", type=Path)
    parser.add_argument("--key", type=Path)
    parser.add_argument("--loopback-cleartext", action="store_true")
    parser.add_argument("--media-config", type=Path)
    parser.add_argument("--media-data", type=Path)
    parser.add_argument("--link-ttl", type=int, default=120)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()
    import os
    try:
        require(2 <= args.workers <= 64, "INVALID_WORKER_LIMIT")
        require(15 <= args.link_ttl <= 300, "INVALID_LINK_TTL")
        key = bytes.fromhex(os.environ.get("DOG_GATEWAY_KEY", ""))
        require(len(key) == 32 and key != chat_feed.key(), "INVALID_GATEWAY_KEY")
        if args.loopback_cleartext:
            require(args.host == "127.0.0.1" and re.fullmatch(r"http://127\.0\.0\.1:[0-9]+", args.origin)
                    and not args.cert and not args.key, "CLEAR_REQUIRES_LOOPBACK")
            context = None
        else:
            require(args.cert and args.key and re.fullmatch(r"https://[a-zA-Z0-9.-]+(?::[0-9]+)?", args.origin),
                    "TLS_REQUIRED")
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(args.cert, args.key)
        require(bool(args.media_config) == bool(args.media_data), "MEDIA_CONFIG_REQUIRED")
        consensus = Consensus(args.config, chat_feed.key(), attempts=4, state_timeout=4)
        with Media.open(args.media_config, args.media_data, consensus, key) as media:
            serve(Gateway(Chat(consensus), key, media, args.link_ttl), context,
                  args.host, args.tcp, args.web, args.origin, args.workers)
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError) as error:
        print(f"Gateway: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
