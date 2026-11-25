"""Codec DM2 binario (protocol/CONTRATO_TCP_v2.md). Solo biblioteca estándar.

Equivalente a NodeMain.MessageCodec de Java. Los mensajes son dataclasses inmutables que
validan sus invariantes al construirse, de modo que decodificar y codificar comparten reglas.
Este módulo no hace I/O: el transporte TCP vive en node.py.
"""
from __future__ import annotations

import re
import struct
from dataclasses import dataclass, field
from typing import Iterable, Optional, Tuple, Union

# --- Constantes del contrato ------------------------------------------------------------
MAX_BODY = 1_048_576
MAGIC = b"DM2\x00"
DM1_PREFIX = b"DM1|"
R64_MAX = (1 << 63) - 1
MAX_APPEND_ENTRIES = 256
MAX_COMMAND = 65_536
HEADER_BYTES = 23  # magic + algorithm + type + requestId + longitud de senderId
APPEND_FIXED_BYTES = 34

RAFT = 0x01

REQUEST_VOTE = 0x01
APPEND_ENTRIES = 0x02
CLIENT_COMMAND = 0x03
VOTE_RESPONSE = 0x81
APPEND_RESPONSE = 0x82
CLIENT_RESULT = 0x83
ERROR = 0xFF
RAFT_REQUESTS = frozenset({REQUEST_VOTE, APPEND_ENTRIES, CLIENT_COMMAND})

UNSUPPORTED_ALGORITHM = 1
UNKNOWN_TYPE = 2
MALFORMED_PAYLOAD = 3
UNKNOWN_MEMBER = 4
ERROR_CODES = frozenset({UNSUPPORTED_ALGORITHM, UNKNOWN_TYPE, MALFORMED_PAYLOAD, UNKNOWN_MEMBER})

NO_OP = 0
COMMAND = 1

COMMITTED = 0
NOT_LEADER = 1
RETRY = 2
REQUEST_ID_CONFLICT = 3

_NODE_ID = re.compile(r"[A-Za-z0-9_-]{1,32}")


# --- Errores ------------------------------------------------------------------------------
class FrameError(Exception):
    """Longitud externa inválida o trama incompleta: cerrar sin respuesta."""


class HeaderError(Exception):
    """Firma o encabezado DM2 inválido: cerrar sin respuesta."""


class PayloadError(Exception):
    """Payload inválido para el tipo (MALFORMED_PAYLOAD en una solicitud)."""


class ResponseError(Exception):
    """Respuesta inválida o no correlacionada: el iniciador la descarta."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def valid_node_id(value: object) -> bool:
    return isinstance(value, str) and _NODE_ID.fullmatch(value) is not None


def _require_node_id(value: object) -> None:
    _require(valid_node_id(value), f"NodeId inválido: {value!r}")


def _require_request_id(value: object) -> None:
    _require(isinstance(value, bytes) and len(value) == 16 and any(value), "requestId nulo o inválido")


def _require_r64(value: object, name: str) -> None:
    _require(isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= R64_MAX,
             f"{name} fuera de R64")


def _require_position(index: int, term: int) -> None:
    _require_r64(index, "index")
    _require_r64(term, "term")
    _require((index == 0) == (term == 0), "LogPosition inválida")


# --- Mensajes -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Header:
    algorithm: int
    type: int
    request_id: bytes
    sender_id: str

    def __post_init__(self) -> None:
        _require(0 <= self.algorithm <= 255 and 0 <= self.type <= 255, "Código fuera de u8")
        _require_request_id(self.request_id)
        _require_node_id(self.sender_id)


@dataclass(frozen=True)
class RequestVote:
    term: int
    last_log_index: int
    last_log_term: int
    TYPE = REQUEST_VOTE

    def __post_init__(self) -> None:
        _require_r64(self.term, "term")
        _require_position(self.last_log_index, self.last_log_term)
        _require(self.term > 0 and self.last_log_term <= self.term, "Término de voto inválido")


@dataclass(frozen=True)
class VoteResponse:
    term: int
    vote_granted: bool
    TYPE = VOTE_RESPONSE

    def __post_init__(self) -> None:
        _require_r64(self.term, "term")
        _require(isinstance(self.vote_granted, bool), "voteGranted debe ser bool")
        _require(not self.vote_granted or self.term > 0, "Término de respuesta de voto inválido")


@dataclass(frozen=True)
class LogEntry:
    """Entrada de log tal como viaja en AppendEntries (sin índice: es implícito)."""
    term: int
    kind: int
    client_id: Optional[str] = None
    client_request_id: Optional[bytes] = None
    command: Optional[bytes] = None

    def __post_init__(self) -> None:
        _require_r64(self.term, "entryTerm")
        _require(self.term > 0, "Término de entrada inválido")
        if self.kind == NO_OP:
            _require(self.client_id is None and self.client_request_id is None and self.command is None,
                     "NO_OP no lleva campos de comando")
        elif self.kind == COMMAND:
            _require_node_id(self.client_id)
            _require_request_id(self.client_request_id)
            _require_command(self.command)
        else:
            raise ValueError("entryKind inválido")

    @staticmethod
    def no_op(term: int) -> "LogEntry":
        return LogEntry(term, NO_OP)

    @staticmethod
    def of_command(term: int, client_id: str, client_request_id: bytes, command: bytes) -> "LogEntry":
        return LogEntry(term, COMMAND, client_id, bytes(client_request_id), bytes(command))


def _require_command(value: object) -> None:
    _require(isinstance(value, bytes) and 1 <= len(value) <= MAX_COMMAND, "Longitud de comando inválida")


@dataclass(frozen=True)
class AppendEntries:
    term: int
    prev_log_index: int
    prev_log_term: int
    leader_commit: int
    entries: Tuple[LogEntry, ...] = field(default_factory=tuple)
    TYPE = APPEND_ENTRIES

    def __post_init__(self) -> None:
        object.__setattr__(self, "entries", tuple(self.entries))
        _require_r64(self.term, "term")
        _require_position(self.prev_log_index, self.prev_log_term)
        _require_r64(self.leader_commit, "leaderCommit")
        _require(self.term > 0 and self.prev_log_term <= self.term, "Término de réplica inválido")
        _require(len(self.entries) <= MAX_APPEND_ENTRIES, "Cantidad de entradas inválida")
        _require(self.prev_log_index <= R64_MAX - len(self.entries), "Índice fuera de R64")
        previous = self.prev_log_term
        for entry in self.entries:
            _require(isinstance(entry, LogEntry), "Entrada inválida")
            _require(previous <= entry.term <= self.term, "Orden de términos inválido")
            previous = entry.term

    @property
    def last_index(self) -> int:
        return self.prev_log_index + len(self.entries)


@dataclass(frozen=True)
class AppendResponse:
    term: int
    success: bool
    match_index: int
    TYPE = APPEND_RESPONSE

    def __post_init__(self) -> None:
        _require_r64(self.term, "term")
        _require_r64(self.match_index, "matchIndex")
        _require(isinstance(self.success, bool), "success debe ser bool")
        _require(self.term > 0 if self.success else self.match_index == 0, "Respuesta AppendEntries inválida")


@dataclass(frozen=True)
class ClientCommand:
    command: bytes
    TYPE = CLIENT_COMMAND

    def __post_init__(self) -> None:
        _require_command(self.command)


@dataclass(frozen=True)
class ClientResult:
    status: int
    log_index: int
    leader_hint: Optional[str] = None
    TYPE = CLIENT_RESULT

    def __post_init__(self) -> None:
        _require_r64(self.log_index, "logIndex")
        _require(self.status in (COMMITTED, NOT_LEADER, RETRY, REQUEST_ID_CONFLICT), "status inválido")
        _require(self.log_index > 0 if self.status == COMMITTED else self.log_index == 0,
                 "Estado o índice de resultado inválido")
        if self.leader_hint is not None:
            _require(self.status == NOT_LEADER, "leaderHint solo con NOT_LEADER")
            _require_node_id(self.leader_hint)


@dataclass(frozen=True)
class ErrorMsg:
    code: int
    TYPE = ERROR

    def __post_init__(self) -> None:
        _require(self.code in ERROR_CODES, "Código ERROR desconocido")


Payload = Union[RequestVote, VoteResponse, AppendEntries, AppendResponse, ClientCommand, ClientResult, ErrorMsg]


@dataclass(frozen=True)
class Message:
    header: Header
    payload: Payload

    def __post_init__(self) -> None:
        _require(self.header.type == self.payload.TYPE, "Tipo y payload incompatibles")
        _require(isinstance(self.payload, ErrorMsg) or self.header.algorithm == RAFT, "Algoritmo no soportado")


# --- Lectura ------------------------------------------------------------------------------
class _Reader:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    def remaining(self) -> int:
        return len(self.data) - self.pos

    def take(self, size: int) -> bytes:
        if size < 0 or size > self.remaining():
            raise ValueError("Datos insuficientes")
        chunk = self.data[self.pos:self.pos + size]
        self.pos += size
        return chunk

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack(">H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def r64(self) -> int:
        value = struct.unpack(">Q", self.take(8))[0]
        _require(value <= R64_MAX, "R64 fuera de rango")
        return value

    def boolean(self) -> bool:
        value = self.u8()
        _require(value <= 1, "Booleano fuera de rango")
        return value == 1

    def node_id(self, optional: bool = False) -> Optional[str]:
        size = self.u8()
        if size == 0 and optional:
            return None
        _require(1 <= size <= 32, "Longitud de ID inválida")
        raw = self.take(size)
        try:
            name = raw.decode("ascii")
        except UnicodeDecodeError as error:
            raise ValueError("ID no ASCII") from error
        _require_node_id(name)
        return name

    def command(self) -> bytes:
        size = self.u32()
        _require(1 <= size <= MAX_COMMAND and size <= self.remaining(), "Longitud de comando inválida")
        return self.take(size)


def frame_length(prefix: bytes) -> int:
    """Valida el prefijo de 4 bytes antes de reservar memoria para el cuerpo."""
    if len(prefix) != 4:
        raise FrameError("Prefijo incompleto")
    length = struct.unpack(">I", prefix)[0]
    if not 1 <= length <= MAX_BODY:
        raise FrameError("Longitud de trama inválida")
    return length


def parse_frame(data: bytes) -> bytes:
    """Extrae el cuerpo de la primera trama de un buffer completo (fin de flujo incluido)."""
    length = frame_length(data[:4])
    if len(data) - 4 < length:
        raise FrameError("EOF prematuro")
    return data[4:4 + length]


def frame(body: bytes) -> bytes:
    if not 1 <= len(body) <= MAX_BODY:
        raise FrameError("Longitud de trama inválida")
    return struct.pack(">I", len(body)) + body


def _read_header(reader: _Reader) -> Header:
    if len(reader.data) > MAX_BODY or reader.data[:4] != MAGIC:
        raise HeaderError("Firma o longitud DM2 inválida")
    try:
        reader.take(4)
        return Header(reader.u8(), reader.u8(), reader.take(16), reader.node_id())
    except ValueError as error:
        raise HeaderError(f"Encabezado DM2 inválido: {error}") from error


def _read_entry(reader: _Reader) -> LogEntry:
    term = reader.r64()
    kind = reader.u8()
    if kind == NO_OP:
        return LogEntry(term, NO_OP)
    if kind == COMMAND:
        return LogEntry(term, COMMAND, reader.node_id(), reader.take(16), reader.command())
    raise ValueError("Tipo de entrada inválido")


def _read_append(reader: _Reader) -> AppendEntries:
    term = reader.r64()
    prev_index, prev_term = reader.r64(), reader.r64()
    commit = reader.r64()
    count = reader.u16()
    _require(count <= MAX_APPEND_ENTRIES and count <= reader.remaining() // 9, "Cantidad de entradas inválida")
    entries = tuple(_read_entry(reader) for _ in range(count))
    return AppendEntries(term, prev_index, prev_term, commit, entries)


def _read_payload(header: Header, reader: _Reader) -> Payload:
    try:
        _require(header.type == ERROR or header.algorithm == RAFT, "Algoritmo no soportado")
        kind = header.type
        payload: Payload
        if kind == REQUEST_VOTE:
            payload = RequestVote(reader.r64(), reader.r64(), reader.r64())
        elif kind == VOTE_RESPONSE:
            payload = VoteResponse(reader.r64(), reader.boolean())
        elif kind == APPEND_ENTRIES:
            payload = _read_append(reader)
        elif kind == APPEND_RESPONSE:
            payload = AppendResponse(reader.r64(), reader.boolean(), reader.r64())
        elif kind == CLIENT_COMMAND:
            payload = ClientCommand(reader.command())
        elif kind == CLIENT_RESULT:
            payload = ClientResult(reader.u8(), reader.r64(), reader.node_id(optional=True))
        elif kind == ERROR:
            payload = ErrorMsg(reader.u8())
        else:
            raise ValueError("Tipo DM2 desconocido")
        _require(reader.remaining() == 0, "Bytes sobrantes")
        return payload
    except ValueError as error:
        raise PayloadError(f"Payload DM2 inválido: {error}") from error


def decode(body: bytes) -> Message:
    """Decodifica un cuerpo completo (sin prefijo). Lanza HeaderError o PayloadError."""
    reader = _Reader(bytes(body))
    header = _read_header(reader)
    return Message(header, _read_payload(header, reader))


# --- Escritura ----------------------------------------------------------------------------
def _put_node(out: bytearray, name: Optional[str]) -> None:
    if name is None:
        out.append(0)
    else:
        raw = name.encode("ascii")
        out.append(len(raw))
        out += raw


def _put_command(out: bytearray, command: bytes) -> None:
    out += struct.pack(">I", len(command))
    out += command


def _put_entry(out: bytearray, entry: LogEntry) -> None:
    out += struct.pack(">QB", entry.term, entry.kind)
    if entry.kind == COMMAND:
        _put_node(out, entry.client_id)
        out += entry.client_request_id
        _put_command(out, entry.command)


def entry_size(entry: LogEntry) -> int:
    if entry.kind == COMMAND:
        return 30 + len(entry.client_id) + len(entry.command)
    return 9


def encode_entry(entry: LogEntry) -> bytes:
    out = bytearray()
    _put_entry(out, entry)
    return bytes(out)


def decode_entry(data: bytes) -> LogEntry:
    reader = _Reader(bytes(data))
    entry = _read_entry(reader)
    _require(reader.remaining() == 0, "Bytes sobrantes en entrada")
    return entry


def encode(message: Message) -> bytes:
    """Codifica el cuerpo DM2 (sin prefijo de longitud)."""
    header, payload = message.header, message.payload
    out = bytearray(MAGIC)
    out += struct.pack(">BB", header.algorithm, header.type)
    out += header.request_id
    _put_node(out, header.sender_id)
    if isinstance(payload, RequestVote):
        out += struct.pack(">QQQ", payload.term, payload.last_log_index, payload.last_log_term)
    elif isinstance(payload, VoteResponse):
        out += struct.pack(">QB", payload.term, int(payload.vote_granted))
    elif isinstance(payload, AppendEntries):
        out += struct.pack(">QQQQH", payload.term, payload.prev_log_index, payload.prev_log_term,
                           payload.leader_commit, len(payload.entries))
        for entry in payload.entries:
            _put_entry(out, entry)
    elif isinstance(payload, AppendResponse):
        out += struct.pack(">QBQ", payload.term, int(payload.success), payload.match_index)
    elif isinstance(payload, ClientCommand):
        _put_command(out, payload.command)
    elif isinstance(payload, ClientResult):
        out += struct.pack(">BQ", payload.status, payload.log_index)
        _put_node(out, payload.leader_hint)
    elif isinstance(payload, ErrorMsg):
        out.append(payload.code)
    else:
        raise ValueError("Payload desconocido")
    if len(out) > MAX_BODY:
        raise ValueError("Cuerpo DM2 excede el máximo")
    return bytes(out)


def encode_frame(message: Message) -> bytes:
    return frame(encode(message))


# --- Servidor: orden de validación de "Rechazos" ------------------------------------------
@dataclass(frozen=True)
class Close:
    """Cerrar la conexión sin respuesta DM2."""
    reason: str


@dataclass(frozen=True)
class Reject:
    """Responder ERROR correlacionado con la solicitud."""
    header: Header
    code: int


@dataclass(frozen=True)
class Accept:
    """Solicitud válida para entregar al motor de consenso."""
    message: Message


Decision = Union[Close, Reject, Accept]


def classify_request(body: bytes, members: Iterable[str]) -> Decision:
    """Aplica los pasos 2 y 3 de "Rechazos" a un cuerpo DM2 ya enmarcado."""
    reader = _Reader(bytes(body))
    try:
        header = _read_header(reader)
    except HeaderError as error:
        return Close(str(error))
    if header.type == 0 or header.type >= 0x80:
        return Close("El servidor solo procesa solicitudes")
    if header.algorithm != RAFT:
        return Reject(header, UNSUPPORTED_ALGORITHM)
    if header.type not in RAFT_REQUESTS:
        return Reject(header, UNKNOWN_TYPE)
    try:
        payload = _read_payload(header, reader)
    except PayloadError:
        return Reject(header, MALFORMED_PAYLOAD)
    if not isinstance(payload, ClientCommand) and header.sender_id not in frozenset(members):
        return Reject(header, UNKNOWN_MEMBER)
    return Accept(Message(header, payload))


def reply(request: Header, payload: Payload, node_id: str) -> bytes:
    """Cuerpo de respuesta: repite requestId y algoritmo, se identifica con node_id."""
    return encode(Message(Header(request.algorithm, payload.TYPE, request.request_id, node_id), payload))


# --- Cliente: correlación y validación de la respuesta ------------------------------------
def check_response(request: Message, expected_peer: str, body: bytes) -> Message:
    """Valida una respuesta frente al intento enviado. Lanza ResponseError si se descarta."""
    try:
        response = decode(body)
    except (HeaderError, PayloadError) as error:
        raise ResponseError(str(error)) from error
    sent, received = request.header, response.header
    if received.request_id != sent.request_id or received.algorithm != sent.algorithm:
        raise ResponseError("Respuesta DM2 sin correlación")
    if received.sender_id != expected_peer:
        raise ResponseError("Emisor inesperado")
    payload = response.payload
    if isinstance(payload, ErrorMsg):
        return response
    if received.type != (sent.type | 0x80):
        raise ResponseError("Tipo de respuesta inesperado")
    original = request.payload
    if isinstance(original, RequestVote) and payload.term < original.term:
        raise ResponseError("Término de voto obsoleto")
    if isinstance(original, AppendEntries):
        if payload.term < original.term or (payload.success and payload.match_index != original.last_index):
            raise ResponseError("Término o matchIndex inesperado")
    return response

