"""Validación determinista del directorio y de sobres opacos E1."""
import base64
import binascii
import hashlib
import re

ID = re.compile(r"[0-9a-f]{32}")
HASH = re.compile(r"[0-9a-f]{64}")
SPKI = bytes.fromhex("3059301306072a8648ce3d020106082a8648ce3d03010703420004")
MAX_DEVICES = 16


def b64(value, size=None):
    if not isinstance(value, str):
        raise ValueError("INVALID_E2EE_ENCODING")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError("INVALID_E2EE_ENCODING") from error
    if base64.b64encode(raw).decode("ascii") != value or (size is not None and len(raw) != size):
        raise ValueError("INVALID_E2EE_ENCODING")
    return raw


def identity(exchange, signing):
    a, b = b64(exchange, 91), b64(signing, 91)
    if not a.startswith(SPKI) or not b.startswith(SPKI) or a == b:
        raise ValueError("INVALID_DEVICE_KEYS")
    return hashlib.sha256(a + b).hexdigest()[:32]


def roster(state, conv):
    return [record for _, record in sorted(state.devices.items())
            if record["active"] and record["user"] in conv["members"]]


def epoch(conv, records):
    # Incluye miembros sin dispositivo y la versión de membresía para evitar ABA.
    payload = str(conv.get("revision", 0)) + "\n" + ",".join(conv["members"])
    payload += "\n" + ",".join(row["id"] for row in records)
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def envelope(body):
    if len(body) > 8192:
        raise ValueError("E2EE_ENVELOPE_TOO_LARGE")
    fields = body.split(".")
    if len(fields) != 5 or fields[0] != "E1":
        raise ValueError("INVALID_E2EE_ENVELOPE")
    b64(fields[1], 12)
    if not 16 < len(b64(fields[2])) <= 4112:
        raise ValueError("INVALID_E2EE_ENVELOPE")
    b64(fields[4], 64)
    wrapped = {}
    for line in fields[3].split(";"):
        parts = line.split(":")
        if len(parts) != 3 or not ID.fullmatch(parts[0]) or parts[0] in wrapped:
            raise ValueError("INVALID_E2EE_ENVELOPE")
        b64(parts[1], 12)
        b64(parts[2], 48)
        wrapped[parts[0]] = True
    if not 0 < len(wrapped) <= MAX_DEVICES or list(wrapped) != sorted(wrapped):
        raise ValueError("INVALID_E2EE_ROSTER")
    return list(wrapped)


def link_time(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{9}", value):
        raise ValueError("INVALID_LINK_TIME")
    return int(value)
