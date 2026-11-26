"""Codec DM2 para el algoritmo 2PC; el codec Raft permanece independiente."""
from __future__ import annotations

from dataclasses import dataclass
import io
import struct

import dm2_codec as dm2

ALGORITHM = 2
SUBMIT, PREPARE, COMMIT, ABORT, QUERY = range(1, 6)
COMMITTED, ABORTED, PENDING, CONFLICT, NOT_COORDINATOR = range(5)
UNKNOWN, DECIDED_COMMIT, DECIDED_ABORT = range(3)


def check(condition, reason="Payload 2PC inválido"):
    if not condition:
        raise ValueError(reason)


def node_bytes(value):
    check(dm2.valid_node_id(value))
    return bytes([len(value)]) + value.encode("ascii")


def token(value):
    check(isinstance(value, bytes) and len(value) == 16 and any(value))
    return value


def position(sequence, allow_zero=False):
    check(type(sequence) is int and int(not allow_zero) <= sequence <= dm2.R64_MAX)
    return struct.pack(">Q", sequence)


def command_bytes(value):
    check(isinstance(value, bytes) and 1 <= len(value) <= dm2.MAX_COMMAND)
    return struct.pack(">I", len(value)) + value


@dataclass(frozen=True)
class Tx:
    sequence: int
    xid: bytes
    coordinator: str
    client: str
    request_id: bytes
    command: bytes

    def encode(self):
        return (position(self.sequence) + token(self.xid) + node_bytes(self.coordinator) +
                node_bytes(self.client) + token(self.request_id) + command_bytes(self.command))


class Reader(io.BytesIO):
    def exact(self, count):
        data = self.read(count)
        check(len(data) == count)
        return data

    def number(self, count):
        return int.from_bytes(self.exact(count), "big")

    def node(self):
        node = self.exact(self.number(1)).decode("ascii")
        node_bytes(node)
        return node

    def sequence(self, zero=False):
        value = self.number(8)
        position(value, zero)
        return value

    def command(self):
        length = self.number(4)
        check(1 <= length <= dm2.MAX_COMMAND)
        return self.exact(length)


def encode(kind, request_id, sender, payload, algorithm=ALGORITHM):
    raw = dm2.MAGIC + bytes((algorithm, kind)) + token(request_id) + node_bytes(sender) + payload
    check(len(raw) <= dm2.MAX_BODY)
    return raw


def header(body):
    check(len(body) <= dm2.MAX_BODY)
    reader = Reader(body)
    check(reader.exact(4) == dm2.MAGIC)
    algorithm, kind = reader.exact(2)
    return algorithm, kind, token(reader.exact(16)), reader.node(), reader


def read_payload(kind, reader):
    if kind == SUBMIT:
        value = reader.command()
    elif kind in (PREPARE, COMMIT, ABORT):
        value = Tx(reader.sequence(), token(reader.exact(16)), reader.node(), reader.node(),
                   token(reader.exact(16)), reader.command())
    elif kind in (QUERY, 0x82, 0x83, 0x84, 0x85):
        value = (reader.sequence(), token(reader.exact(16)))
        if kind != QUERY:
            status = reader.number(1)
            check(status <= (2 if kind == 0x85 else 1))
            value += (status,)
    elif kind == 0x81:
        status, sequence, leader = reader.number(1), reader.sequence(True), reader.node()
        check(status <= 4 and (sequence > 0 if status <= 1 else sequence == 0 if status >= 3 else True))
        value = status, sequence, leader
    elif kind == 0xff:
        value = reader.number(1)
        check(value in (1, 2, 3, 4))
    else:
        raise ValueError("Tipo 2PC no reconocido")
    check(not reader.read(1))
    return value


def correlate(body, expected, request_id, kind, transaction=None):
    algorithm, response, found_id, sender, reader = header(body)
    check((algorithm, response, found_id, sender) == (ALGORITHM, kind | 0x80, request_id, expected),
          "Respuesta 2PC no correlacionada")
    value = read_payload(response, reader)
    if transaction is not None:
        check(value[:2] == transaction, "Transacción en respuesta 2PC incorrecta")
    return value
