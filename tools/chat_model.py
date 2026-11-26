"""Proyección determinista de Dog Messenger sobre operaciones aplicadas."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import re
import secrets
import struct
import uuid

import e2ee_model as secure

MAGIC = b"CHAT1"
COUNTS = {1: 6, 2: 2, 3: 1, 4: 2, 5: 4, 6: 3, 7: 3, 8: 4, 9: 1, 10: 9, 11: 4, 12: 2, 13: 2, 14: 6, 15: 12, 16: 7, 17: 4, 18: 2, 19: 5}
REGISTER, SESSION, REVOKE, DIRECT, GROUP, ADD, REMOVE, SEND, BARRIER, ATTACH, DEVICE, DEVICE_REVOKE, SECURE_ENABLE, SECURE_SEND, SECURE_ATTACH, LINK_START, LINK_APPROVE, LINK_CANCEL, HISTORY_GRANT = COUNTS
ITERATIONS = 600_000
MAX_TEXT = 4096
ID = re.compile(r"[0-9a-f]{32}")
HASH = re.compile(r"[0-9a-f]{64}")
USERNAME = re.compile(r"[a-z0-9_]{3,24}")


class Rejected(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise Rejected(reason)


@dataclass(frozen=True)
class Operation:
    kind: int
    request: bytes
    fields: tuple[str, ...]

    def encode(self, secret):
        require(self.kind in COUNTS and COUNTS[self.kind] == len(self.fields), "INVALID_OPERATION")
        require(len(self.request) == 16 and any(self.request), "INVALID_ID")
        payload = bytearray(MAGIC + bytes([self.kind]) + self.request)
        for field in self.fields:
            encoded = field.encode("utf-8", errors="strict")
            require(len(encoded) <= 8192, "FIELD_TOO_LONG")
            payload.extend(struct.pack(">H", len(encoded)))
            payload.extend(encoded)
        require(len(payload) + 32 <= 65536, "OPERATION_TOO_LONG")
        return bytes(payload) + hmac.digest(secret, payload, "sha256")


def decode(raw, secret):
    require(raw.startswith(MAGIC) and 54 <= len(raw) <= 65536, "INVALID_VERSION")
    payload, signature = raw[:-32], raw[-32:]
    require(hmac.compare_digest(signature, hmac.digest(secret, payload, "sha256")), "UNAUTHORIZED_OPERATION")
    kind, request = payload[5], payload[6:22]
    require(kind in COUNTS and any(request), "INVALID_OPERATION")
    fields, offset = [], 22
    for _ in range(COUNTS[kind]):
        require(offset + 2 <= len(payload), "TRUNCATED_FIELD")
        length = int.from_bytes(payload[offset:offset + 2], "big")
        offset += 2
        require(length <= 8192 and offset + length <= len(payload), "INVALID_FIELD_LENGTH")
        try:
            fields.append(payload[offset:offset + length].decode("utf-8", errors="strict"))
        except UnicodeError as error:
            raise Rejected("INVALID_UTF8") from error
        offset += length
    require(offset == len(payload), "UNKNOWN_FIELDS")
    return Operation(kind, request, tuple(fields))


def valid_filename(name):
    return (bool(name.strip()) and name not in {".", ".."} and len(name.encode("utf-8")) <= 240
            and not any(c in '/\\:' or ord(c) < 32 or 127 <= ord(c) < 160
                        or 0x202A <= ord(c) <= 0x202E or 0x2066 <= ord(c) <= 0x2069 for c in name))


def identity():
    return uuid.uuid4().hex


def verifier(password, salt, iterations=ITERATIONS):
    try:
        raw = password.encode("utf-8")
    except UnicodeError as error:
        raise Rejected("INVALID_PASSWORD") from error
    require(8 <= len(raw) <= 1024, "INVALID_PASSWORD")
    return hashlib.pbkdf2_hmac("sha256", raw, salt, iterations).hex()


def new_credential(password):
    salt = secrets.token_bytes(16)
    return salt.hex(), str(ITERATIONS), verifier(password, salt)


def new_session():
    token = secrets.token_bytes(32)
    return token.hex(), hashlib.sha256(token).hexdigest()


def session_hash(token):
    require(isinstance(token, str) and HASH.fullmatch(token), "AUTH_REQUIRED")
    return hashlib.sha256(bytes.fromhex(token)).hexdigest()


class ChatState:
    def __init__(self, key):
        self.key = key
        self.cursor = 0
        self.users = {}
        self.names = {}
        self.sessions = {}
        self.conversations = {}
        self.messages = {}
        self.results = {}
        self.devices = {}
        self.device_sessions = {}
        self.links = {}
        self.history_grants = {}

    def apply(self, position, raw):
        require(position > self.cursor, "OUT_OF_ORDER")
        self.cursor = position
        try:
            op = decode(raw, self.key)
        except Rejected:
            # Otros comandos o un sobre no autorizado no tienen efecto de aplicación.
            return None
        digest = hashlib.sha256(raw).hexdigest()
        previous = self.results.get(op.request.hex())
        if previous is not None:
            return previous["result"] if previous["digest"] == digest else {"status": "REJECTED", "reason": "ID_CONFLICT"}
        owner = None
        try:
            if op.kind == REGISTER:
                owner = op.fields[0]
                data = self.register(*op.fields)
            elif op.kind == SESSION:
                owner, token = op.fields
                require(owner in self.users and HASH.fullmatch(token), "INVALID_SESSION")
                require(token not in self.sessions, "SESSION_EXISTS")
                self.sessions[token] = owner
                data = {}
            else:
                owner = self.actor(op.fields[0])
                data = self.modify(op.kind, owner, op.fields, position, op.request.hex())
            result = {"status": "APPLIED", "position": position, **data}
        except Rejected as error:
            result = {"status": "REJECTED", "position": position, "reason": str(error)}
        self.results[op.request.hex()] = {"digest": digest, "owner": owner, "result": result}
        return result

    def register(self, userid, username, display, salt, iterations, credential):
        require(ID.fullmatch(userid) and USERNAME.fullmatch(username), "INVALID_USER")
        require(display.strip() and len(display.encode()) <= 128, "INVALID_DISPLAY")
        require(ID.fullmatch(salt) and HASH.fullmatch(credential) and iterations == str(ITERATIONS),
                "INVALID_CREDENTIAL_FORMAT")
        require(userid not in self.users and username not in self.names, "USER_EXISTS")
        self.users[userid] = {"id": userid, "username": username, "display": display, "salt": salt,
                              "credential": credential, "iterations": ITERATIONS, "state": "ACTIVE"}
        self.names[username] = userid
        return {"user": userid}

    def authenticate(self, username, password):
        record = self.users.get(self.names.get(username))
        salt = bytes.fromhex(record["salt"]) if record else bytes(16)
        candidate = verifier(password, salt)
        matches = hmac.compare_digest(candidate, record["credential"] if record else "0" * 64)
        require(record is not None and matches, "INVALID_CREDENTIALS")
        return record["id"]

    def actor(self, token_hash):
        require(isinstance(token_hash, str) and HASH.fullmatch(token_hash) and token_hash in self.sessions,
                "AUTH_REQUIRED")
        device = self.device_sessions.get(token_hash)
        require(device is None or self.devices[device]["active"], "AUTH_REQUIRED")
        return self.sessions[token_hash]

    def allowed(self, actor, cid):
        conv = self.conversations.get(cid)
        require(conv is not None and actor in conv["members"], "FORBIDDEN")
        return conv

    def modify(self, kind, actor, fields, position, request):
        token, *args = fields
        if kind == REVOKE:
            del self.sessions[token]
            self.device_sessions.pop(token, None)
            return {}
        if kind == BARRIER:
            return {}
        if kind == DEVICE:
            device, exchange, signing = args
            try:
                expected = secure.identity(exchange, signing)
            except ValueError as error:
                raise Rejected(str(error)) from error
            require(device == expected, "INVALID_DEVICE_ID")
            existing = self.devices.get(device)
            if existing is None:
                require(not any(row["user"] == actor for row in self.devices.values()), "DEVICE_LINK_REQUIRED")
                self.devices[device] = {"id": device, "user": actor, "exchange": exchange,
                                        "signing": signing, "active": True}
            else:
                require(existing["active"] and existing["user"] == actor, "DEVICE_UNAVAILABLE")
            require(self.device_sessions.get(token, device) == device, "DEVICE_SESSION_CONFLICT")
            self.device_sessions[token] = device
            return {"device": device}
        if kind == DEVICE_REVOKE:
            device, = args
            row = self.devices.get(device)
            require(row is not None and row["user"] == actor and row["active"], "DEVICE_UNAVAILABLE")
            row["active"] = False
            for session, bound in list(self.device_sessions.items()):
                if bound == device:
                    self.sessions.pop(session, None)
                    del self.device_sessions[session]
            return {"device": device}
        if kind == LINK_START:
            device, exchange, signing, challenge, issued, expires = args
            try:
                expected = secure.identity(exchange, signing)
                start, end = secure.link_time(issued), secure.link_time(expires)
            except ValueError as error:
                raise Rejected(str(error)) from error
            require(expected == device and device not in self.devices and re.fullmatch(r"[0-9a-f]{64}", challenge)
                    and 15 <= end - start <= 300, "INVALID_LINK")
            require(token not in self.device_sessions and any(
                row["active"] and row["user"] == actor for row in self.devices.values()), "LINK_NOT_NEEDED")
            pending = [link for link in self.links.values()
                       if link["user"] == actor and link["status"] == "PENDING" and link["expires"] > start]
            require(len(pending) < 4 and not any(link["device"] == device or link["candidate"] == token
                                                for link in pending), "LINK_LIMIT")
            self.links[request] = {"id": request, "user": actor, "candidate": token, "device": device,
                                   "exchange": exchange, "signing": signing, "challenge": challenge,
                                   "issued": start, "expires": end, "status": "PENDING"}
            return {"link": request}
        if kind in (LINK_APPROVE, LINK_CANCEL):
            link = self.links.get(args[0])
            require(link is not None and link["user"] == actor, "UNKNOWN_LINK")
            require(link["status"] == "PENDING", "LINK_USED")
            if kind == LINK_CANCEL:
                require(token == link["candidate"] or token in self.device_sessions, "DEVICE_REQUIRED")
                link["status"] = "CANCELLED"
                return {"link": link["id"]}
            issuer, observed = args[1:]
            require(self.device_sessions.get(token) == issuer, "DEVICE_REQUIRED")
            try:
                clock = secure.link_time(observed)
            except ValueError as error:
                raise Rejected(str(error)) from error
            require(link["issued"] <= clock < link["expires"], "LINK_EXPIRED")
            device = link["device"]
            candidate = link["candidate"]
            require(device not in self.devices and self.sessions.get(candidate) == actor
                    and candidate not in self.device_sessions, "LINK_UNAVAILABLE")
            require(sum(1 for row in self.devices.values() if row["user"] == actor and row["active"]) < 4,
                    "DEVICE_LIMIT")
            self.devices[device] = {name: link[name] for name in ("user", "exchange", "signing")}
            self.devices[device].update(id=device, active=True)
            self.device_sessions[candidate] = device
            link.update(status="LINKED", approval=request, approver=issuer, approved_at=clock)
            return {"link": link["id"], "device": device}
        if kind == HISTORY_GRANT:
            mid, issuer, target, body = args
            message = self.messages.get(mid)
            require(message is not None and message["type"] in ("E2EE", "E2EE_FILE"), "UNKNOWN_MESSAGE")
            self.allowed(actor, message["conversation"])
            require(self.device_sessions.get(token) == issuer and issuer != target, "DEVICE_REQUIRED")
            destination = self.devices.get(target)
            require(destination is not None and destination["active"] and destination["user"] == actor,
                    "DEVICE_UNAVAILABLE")
            try:
                require(issuer in secure.envelope(message["envelope"])
                        and secure.envelope(body) == [target], "INVALID_HISTORY_GRANT")
            except ValueError as error:
                raise Rejected(str(error)) from error
            source = self.devices[issuer]
            per_message = self.history_grants.setdefault(mid, {})
            require(target not in per_message, "HISTORY_GRANT_EXISTS")
            per_message[target] = {"issuer": issuer, "exchange": source["exchange"],
                                   "signing": source["signing"], "envelope": body}
            return {"message": mid, "device": target}
        if kind == SECURE_ENABLE:
            cid, = args
            conv = self.allowed(actor, cid)
            require(conv["type"] == "DIRECT" or conv["owner"] == actor, "ADMIN_REQUIRED")
            self.secure_roster(conv)
            require(token in self.device_sessions, "DEVICE_REQUIRED")
            conv["e2ee"] = True
            return {"conversation": cid}
        if kind == SECURE_SEND:
            cid, mid, device, epoch, body = args
            conv = self.allowed(actor, cid)
            require(mid == request and mid not in self.messages, "MESSAGE_ID_CONFLICT")
            row = self.secure_message(conv, actor, token, device, epoch, body)
            self.messages[mid] = {"id": mid, "conversation": cid, "sender": actor, "request_id": request,
                                  "type": "E2EE", "position": position, "device": device, "epoch": epoch,
                                  "envelope": body, "sender_keys": {"exchange": row["exchange"],
                                                                   "signing": row["signing"]}}
            conv["messages"].append(mid)
            return {"message": mid}
        if kind == DIRECT:
            peer, = args
            require(peer in self.users and peer != actor, "INVALID_PEER")
            members = sorted((actor, peer))
            cid = hashlib.sha256(("direct:" + ":".join(members)).encode()).hexdigest()[:32]
            self.conversations.setdefault(cid, {"id": cid, "type": "DIRECT", "members": members,
                                                "owner": None, "name": None, "messages": []})
            return {"conversation": cid}
        if kind == GROUP:
            cid, name, identifiers = args
            members = set(identifiers.split(",")) if identifiers else set()
            members.add(actor)
            require(ID.fullmatch(cid) and cid not in self.conversations and name.strip()
                    and len(name.encode()) <= 128, "INVALID_GROUP")
            require(len(members) <= 64 and members <= self.users.keys(), "INVALID_MEMBERS")
            self.conversations[cid] = {"id": cid, "type": "GROUP", "members": sorted(members),
                                       "owner": actor, "name": name, "messages": []}
            return {"conversation": cid}
        if kind in (ADD, REMOVE):
            cid, member = args
            conv = self.allowed(actor, cid)
            require(conv["type"] == "GROUP" and conv["owner"] == actor, "ADMIN_REQUIRED")
            require(member in self.users and member != actor, "INVALID_MEMBER")
            members = set(conv["members"])
            if kind == ADD:
                require(member in members or len(members) < 64, "GROUP_FULL")
                members.add(member)
            else:
                members.discard(member)
            if conv["members"] != sorted(members):
                if conv.get("e2ee") and kind == ADD:
                    require(any(row["active"] and row["user"] == member for row in self.devices.values()),
                            "MISSING_RECIPIENT_DEVICE")
                conv["members"] = sorted(members)
                conv["revision"] = conv.get("revision", 0) + 1
            return {}
        if kind in (ATTACH, SECURE_ATTACH):
            cid, mid, media_type, filename, content_type, size, sha, proof = args[:8]
            conv = self.allowed(actor, cid)
            require(ID.fullmatch(mid) and mid not in self.messages, "MESSAGE_ID_CONFLICT")
            require(HASH.fullmatch(sha) and re.fullmatch(r"[1-9][0-9]{0,7}", size)
                    and int(size) <= 50 * 1024 * 1024, "INVALID_ATTACHMENT")
            extra = {}
            if kind == SECURE_ATTACH:
                require(mid == request and (media_type, filename, content_type) ==
                        ("E2EE_FILE", "adjunto.dgf", "application/octet-stream"), "INVALID_ATTACHMENT")
                device, epoch, body = args[8:]
                row = self.secure_message(conv, actor, token, device, epoch, body)
                extra = {"device": device, "epoch": epoch, "envelope": body,
                         "sender_keys": {"exchange": row["exchange"], "signing": row["signing"]}}
            else:
                require(not conv.get("e2ee"), "E2EE_REQUIRED")
                require(valid_filename(filename) and media_type in ("IMAGE", "FILE"), "INVALID_ATTACHMENT")
                accepted = ("image/png", "image/jpeg") if media_type == "IMAGE" else (
                    "application/pdf", "application/zip", "application/octet-stream")
                require(content_type in accepted, "INVALID_ATTACHMENT")
            receipts = proof.split(",")
            require(2 <= len(receipts) <= 3 and len({row[:2] for row in receipts}) == len(receipts)
                    and all(re.fullmatch(r"n[123]:[0-9a-f]{64}", row) for row in receipts), "INVALID_RECEIPTS")
            self.messages[mid] = {"id": mid, "conversation": cid, "sender": actor, "request_id": request,
                                  "type": media_type, "position": position, **extra,
                                  "attachment": {"id": mid, "filename": filename, "content_type": content_type,
                                                 "size": int(size), "sha256": sha, "storage": sha}}
            conv["messages"].append(mid)
            return {"message": mid}
        cid, mid, text = args
        conv = self.allowed(actor, cid)
        require(not conv.get("e2ee"), "E2EE_REQUIRED")
        require(ID.fullmatch(mid) and mid not in self.messages, "MESSAGE_ID_CONFLICT")
        require(text.strip() and len(text.encode("utf-8")) <= MAX_TEXT, "INVALID_TEXT")
        self.messages[mid] = {"id": mid, "conversation": cid, "sender": actor, "request_id": request,
                              "type": "TEXT", "text": text, "position": position}
        conv["messages"].append(mid)
        return {"message": mid}

    def secure_message(self, conv, actor, session, device, epoch, body):
        require(conv.get("e2ee"), "E2EE_NOT_ENABLED")
        row = self.devices.get(device)
        require(row is not None and row["active"] and row["user"] == actor
                and self.device_sessions.get(session) == device, "DEVICE_REQUIRED")
        records = self.secure_roster(conv)
        require(epoch == secure.epoch(conv, records), "E2EE_EPOCH_CHANGED")
        try:
            recipients = secure.envelope(body)
        except ValueError as error:
            raise Rejected(str(error)) from error
        require(recipients == [item["id"] for item in records], "E2EE_ROSTER_CHANGED")
        return row

    def secure_roster(self, conv):
        records = secure.roster(self, conv)
        require({row["user"] for row in records} == set(conv["members"]), "MISSING_RECIPIENT_DEVICE")
        require(0 < len(records) <= secure.MAX_DEVICES, "E2EE_ROSTER_TOO_LARGE")
        return records

    def secure_directory(self, session, cid):
        conv = self.allowed(self.actor(session), cid)
        records = self.secure_roster(conv)
        return {"epoch": secure.epoch(conv, records), "devices": records, "enabled": bool(conv.get("e2ee"))}

    def query(self, session, name, cid=None, after=0, limit=50, request=None):
        actor = self.actor(session)
        if name == "users":
            return [{field: user[field] for field in ("id", "username", "display", "state")}
                    for _, user in sorted(self.users.items())]
        if name in ("groups", "conversations"):
            return [{k: v for k, v in conv.items() if k != "messages"}
                    for _, conv in sorted(self.conversations.items())
                    if actor in conv["members"] and (name != "groups" or conv["type"] == "GROUP")]
        if name == "status":
            found = self.results.get(request)
            if found is None:
                return {"status": "UNKNOWN"}
            require(actor == found["owner"], "FORBIDDEN")
            return found["result"]
        conv = self.allowed(actor, cid)
        if name == "members":
            return list(conv["members"])
        require(name in ("history", "sync") and 0 <= after <= self.cursor and 1 <= limit <= 100, "INVALID_CURSOR")
        entries = [self.messages[mid] for mid in conv["messages"] if self.messages[mid]["position"] > after]
        page = entries[:limit]
        device = self.device_sessions.get(session)
        if device is not None:
            page = [({**entry, "history_key": grant} if (grant := self.history_grants.get(entry["id"], {}).get(device))
                     else entry) for entry in page]
        return {"messages": page, "next_cursor": page[-1]["position"] if len(entries) > limit else self.cursor,
                "more": len(entries) > limit}

    def public_digest(self):
        # Excluye credenciales, verificadores de sesión, firmas y tokens.
        view = {"users": [{k: v for k, v in row.items() if k not in ("salt", "credential", "iterations")}
                          for _, row in sorted(self.users.items())],
                "conversations": dict(sorted(self.conversations.items())),
                "messages": dict(sorted(self.messages.items())),
                "results": {key: value["result"] for key, value in sorted(self.results.items())}}
        if self.devices:
            view["devices"] = self.devices
        if self.links:
            view["links"] = {ident: {k: v for k, v in row.items() if k != "candidate"}
                             for ident, row in self.links.items()}
        if self.history_grants:
            view["history_grants"] = self.history_grants
        return hashlib.sha256(json.dumps(view, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
