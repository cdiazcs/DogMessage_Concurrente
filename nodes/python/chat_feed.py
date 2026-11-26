"""Feed local de operaciones aplicadas, separado del protocolo de consenso."""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import struct

import two_pc_wire as w
import dm2_codec as dm2

ALGORITHM = 3
MAGIC = b"CHAT1"
DOMAIN = b"DM2-chat-feed-v1"


def key():
    value = os.environ.get("DOG_CHAT_KEY", "")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ValueError("DOG_CHAT_KEY debe contener 32 bytes hex")
    return bytes.fromhex(value)


def signature(secret, request, cursor):
    return hmac.digest(secret, DOMAIN + request + struct.pack(">Q", cursor), hashlib.sha256)


def is_feed(body):
    return body.startswith(dm2.MAGIC + bytes([ALGORITHM]))


def diagnostic_bytes(body):
    return "REDACTED_CHAT" if body.startswith(MAGIC) else body.hex()


def dispatch(body, node_id, secret, snapshot):
    try:
        algorithm, kind, request, _, reader = w.header(body)
        if (algorithm, kind) != (ALGORITHM, 1):
            return None
        after = reader.sequence(True)
        sig = reader.exact(32)
        if reader.read(1) or not hmac.compare_digest(sig, signature(secret, request, after)):
            return None
    except ValueError:
        return None
    cursor, entries = snapshot(after)
    selected = []
    size = 23 + len(node_id) + 10
    for position, payload in entries:
        if position <= after or not payload.startswith(MAGIC):
            continue
        if len(selected) == 64 or size + 12 + len(payload) > dm2.MAX_BODY:
            cursor = selected[-1][0]
            break
        selected.append((position, payload))
        size += 12 + len(payload)
    payload = struct.pack(">QH", cursor, len(selected)) + b"".join(
        struct.pack(">QI", position, len(raw)) + raw for position, raw in selected)
    return w.encode(0x81, request, node_id, payload, ALGORITHM)
