"""Dog Messenger PC4: nodo TCP Python con listener mixto DM1 + DM2 binario. Solo SDK.

Equivalente a TcpServer, TcpClient, FramedConnection y RpcDispatcher de NodeMain.java:
- Una solicitud por conexión; el servidor responde (o no) y cierra.
- Plazo de conexión de 5 s y presupuesto de lectura de 5 s por trama (prefijo + cuerpo),
  medido con time.monotonic.
- Un hilo por conexión, acotado por un threading.Semaphore; si no hay cupo se cierra.
"""
from __future__ import annotations

import argparse
import os
import re
import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dm2_codec as dm2  # noqa: E402

IO_TIMEOUT = 5.0
MAX_CONNECTIONS = 32
MAX_FRAME = dm2.MAX_BODY
ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,32}$")
REQ_RE = re.compile(r"^[a-fA-F0-9]{32}$")

Endpoint = Tuple[str, int]


class TransportError(OSError):
    """Fallo de transporte o respuesta descartada: el intento RPC no tuvo resultado."""


# --- Framing ------------------------------------------------------------------------------
def recv_exact(conn: socket.socket, size: int, deadline: float) -> bytes:
    """Lee exactamente `size` bytes antes de `deadline` (time.monotonic), tolerando fragmentación."""
    chunks = []
    while size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("Plazo de trama agotado")
        conn.settimeout(remaining)
        part = conn.recv(min(size, 65_536))
        if not part:
            raise ConnectionError("Conexión cerrada antes de completar trama")
        chunks.append(part)
        size -= len(part)
    return b"".join(chunks)


def read_frame(conn: socket.socket, timeout: float = IO_TIMEOUT) -> bytes:
    """Lee una trama completa. Un único presupuesto cubre prefijo y cuerpo."""
    deadline = time.monotonic() + timeout
    length = dm2.frame_length(recv_exact(conn, 4, deadline))
    return recv_exact(conn, length, deadline)


def write_frame(conn: socket.socket, body: bytes, timeout: float = IO_TIMEOUT) -> None:
    conn.settimeout(timeout)  # Desde 3.5 sendall aplica el plazo al envío completo.
    conn.sendall(dm2.frame(body))


# Compatibilidad DM1 (tests/test_protocol.py y diagnóstico).
def encode_frame(body: str) -> bytes:
    try:
        return dm2.frame(body.encode("utf-8"))
    except dm2.FrameError as error:
        raise ValueError(str(error)) from error


def recv_frame(conn: socket.socket, timeout: float = IO_TIMEOUT) -> str:
    try:
        return read_frame(conn, timeout).decode("utf-8", errors="strict")
    except dm2.FrameError as error:
        raise ValueError(str(error)) from error


# --- Dispatcher -----------------------------------------------------------------------------
class RpcHandler:
    """Invocado concurrentemente. None indica que no puede emitirse respuesta (se cierra)."""

    def request_vote(self, header: dm2.Header, request: dm2.RequestVote) -> Optional[dm2.VoteResponse]:
        return None

    def append_entries(self, header: dm2.Header, request: dm2.AppendEntries) -> Optional[dm2.AppendResponse]:
        return None

    def client_command(self, header: dm2.Header, request: dm2.ClientCommand) -> Optional[dm2.ClientResult]:
        return None


class Dispatcher:
    def __init__(self, node_id: str, members: Iterable[str], handler: RpcHandler) -> None:
        if not dm2.valid_node_id(node_id):
            raise ValueError("ID de nodo inválido")
        self.node_id = node_id
        self.members = frozenset(members)
        if not all(dm2.valid_node_id(m) for m in self.members) or node_id not in self.members:
            raise ValueError("La membresía debe incluir al nodo local y usar IDs válidos")
        self.handler = handler

    def dispatch(self, body: bytes) -> Optional[bytes]:
        """Devuelve el cuerpo de respuesta o None para cerrar sin responder."""
        if body.startswith(dm2.DM1_PREFIX):
            return self._ping(body)
        decision = dm2.classify_request(body, self.members)
        if isinstance(decision, dm2.Close):
            return None
        if isinstance(decision, dm2.Reject):
            return dm2.reply(decision.header, dm2.ErrorMsg(decision.code), self.node_id)
        header, request = decision.message.header, decision.message.payload
        if isinstance(request, dm2.RequestVote):
            response = self.handler.request_vote(header, request)
        elif isinstance(request, dm2.AppendEntries):
            response = self.handler.append_entries(header, request)
        else:
            response = self.handler.client_command(header, request)
        if response is None:
            return None
        self._check_own_response(request, response)
        return dm2.reply(header, response, self.node_id)

    def _check_own_response(self, request, response) -> None:
        """Igual que validateResponse de Java: nunca emitir una respuesta incoherente."""
        if response.TYPE != (request.TYPE | 0x80):
            raise ValueError("Tipo de respuesta inesperado")
        if isinstance(request, dm2.RequestVote) and response.term < request.term:
            raise ValueError("Término de voto obsoleto")
        if isinstance(request, dm2.AppendEntries) and (
                response.term < request.term or (response.success and response.match_index != request.last_index)):
            raise ValueError("Término o matchIndex inesperado")
        if isinstance(response, dm2.ClientResult) and response.leader_hint is not None \
                and response.leader_hint not in self.members:
            raise ValueError("Líder sugerido desconocido")

    def _ping(self, body: bytes) -> bytes:
        parts = body.decode("utf-8", errors="replace").split("|")
        if len(parts) == 4 and parts[1] == "PING" and ID_RE.fullmatch(parts[2]) and REQ_RE.fullmatch(parts[3]):
            text = f"DM1|PONG|{self.node_id}|{parts[3]}"
        else:
            text = f"DM1|ERROR|{self.node_id}|{'0' * 32}"
        return text.encode("ascii")


# --- Servidor ------------------------------------------------------------------------------
class TcpServer:
    def __init__(self, host: str, port: int, dispatcher: Dispatcher,
                 max_connections: int = MAX_CONNECTIONS, timeout: float = IO_TIMEOUT) -> None:
        if max_connections <= 0 or timeout <= 0:
            raise ValueError("Capacidades TCP inválidas")
        self.dispatcher = dispatcher
        self.timeout = timeout
        self._slots = threading.Semaphore(max_connections)
        self._lock = threading.Lock()
        self._clients: Set[socket.socket] = set()
        self._closed = threading.Event()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if os.name != "nt":
                # En Windows SO_REUSEADDR permite robar un puerto en uso; allí no se activa.
                self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._listener.bind((host, port))
            self._listener.listen(64)
            self._listener.settimeout(0.25)  # Permite observar close() sin depender de la plataforma.
        except OSError:
            self._listener.close()
            raise

    @property
    def port(self) -> int:
        return self._listener.getsockname()[1]

    def serve_forever(self) -> None:
        while not self._closed.is_set():
            try:
                conn, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._closed.is_set():
                    return
                raise
            self._submit(conn)

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self.serve_forever, name="dm-listener", daemon=True)
        thread.start()
        return thread

    def _submit(self, conn: socket.socket) -> None:
        if not self._slots.acquire(blocking=False):
            conn.close()  # Saturado: igual que el rechazo del ThreadPoolExecutor de Java.
            return
        with self._lock:
            if self._closed.is_set():
                self._slots.release()
                conn.close()
                return
            self._clients.add(conn)
        try:
            threading.Thread(target=self._handle, args=(conn,), name="dm-conn", daemon=True).start()
        except RuntimeError:
            self._release(conn)

    def _handle(self, conn: socket.socket) -> None:
        try:
            body = read_frame(conn, self.timeout)
            response = self.dispatcher.dispatch(body)
            if response is not None:
                write_frame(conn, response, self.timeout)
        except (OSError, dm2.FrameError, ValueError):
            pass  # Un fallo de transporte o del manejador termina únicamente este intento.
        except Exception as error:  # noqa: BLE001 - nunca matar el listener por un manejador
            print(f"[PYTHON {self.dispatcher.node_id}] error atendiendo conexión: {error!r}", file=sys.stderr)
        finally:
            self._release(conn)

    def _release(self, conn: socket.socket) -> None:
        with self._lock:
            self._clients.discard(conn)
        try:
            conn.close()
        finally:
            self._slots.release()

    def close(self) -> None:
        with self._lock:
            if self._closed.is_set():
                return
            self._closed.set()
            clients = list(self._clients)
        self._listener.close()
        for conn in clients:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def __enter__(self) -> "TcpServer":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# --- Cliente -------------------------------------------------------------------------------
def new_request_id() -> bytes:
    while True:
        value = os.urandom(16)
        if any(value):
            return value


class TcpClient:
    """Una conexión nueva por intento. El emisor esperado sale de la configuración del llamador."""

    def __init__(self, timeout: float = IO_TIMEOUT) -> None:
        self.timeout = timeout

    def exchange(self, endpoint: Endpoint, body: bytes) -> bytes:
        try:
            with socket.create_connection(endpoint, timeout=self.timeout) as conn:
                write_frame(conn, body, self.timeout)
                return read_frame(conn, self.timeout)
        except dm2.FrameError as error:
            raise TransportError(str(error)) from error

    def call(self, endpoint: Endpoint, expected_peer: str, request: dm2.Message) -> dm2.Message:
        if not dm2.valid_node_id(expected_peer):
            raise ValueError("Peer esperado inválido")
        if request.header.type not in dm2.RAFT_REQUESTS:
            raise ValueError("Se requiere una solicitud DM2")
        response_body = self.exchange(endpoint, dm2.encode(request))
        try:
            return dm2.check_response(request, expected_peer, response_body)
        except dm2.ResponseError as error:
            raise TransportError(f"Respuesta descartada: {error}") from error

    def call_rpc(self, endpoint: Endpoint, expected_peer: str, sender_id: str, payload) -> dm2.Message:
        """RPC entre réplicas: cada intento usa un requestId nuevo."""
        if not isinstance(payload, (dm2.RequestVote, dm2.AppendEntries)):
            raise ValueError("Los comandos de cliente requieren un requestId explícito para reintentos")
        header = dm2.Header(dm2.RAFT, payload.TYPE, new_request_id(), sender_id)
        return self.call(endpoint, expected_peer, dm2.Message(header, payload))

    def ping(self, endpoint: Endpoint, sender_id: str) -> str:
        if not ID_RE.fullmatch(sender_id):
            raise ValueError("ID inválido")
        request_id = uuid.uuid4().hex
        body = f"DM1|PING|{sender_id}|{request_id}".encode("ascii")
        parts = self.exchange(endpoint, body).decode("utf-8", errors="replace").split("|")
        if len(parts) != 4 or parts[0] != "DM1" or parts[1] != "PONG" or not ID_RE.fullmatch(parts[2]) \
                or parts[3] != request_id:
            raise TransportError(f"Respuesta PONG inesperada: {parts}")
        return parts[2]


# --- CLI -----------------------------------------------------------------------------------
def parse_endpoint(text: str) -> Endpoint:
    host, separator, port = text.rpartition(":")
    if not separator or not host:
        raise ValueError(f"Endpoint inválido {text!r}; usa HOST:PUERTO")
    value = int(port)
    if not 0 < value < 65_536:
        raise ValueError("Puerto fuera de rango")
    return host, value


def parse_peer(text: str) -> Tuple[str, Endpoint]:
    peer, separator, endpoint = text.partition("=")
    if not separator or not ID_RE.fullmatch(peer):
        raise ValueError("Usa --peer ID=HOST:PUERTO")
    return peer, parse_endpoint(endpoint)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nodo Dog Messenger (Python): DM1 + DM2 binario")
    parser.add_argument("--id", required=True, dest="node_id")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9102)
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--peer", action="append", default=[], metavar="ID=HOST:PUERTO")
    parser.add_argument("--members", help="IDs separados por coma (por defecto: --id y los --peer)")
    parser.add_argument("--ping", metavar="HOST:PUERTO")
    parser.add_argument("--inspect", action="store_true", help="Imprime el estado Raft persistido y sale")
    parser.add_argument("--algo", choices=["raft"], default="raft", help="2PC llega en la fase III")
    return parser


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not ID_RE.fullmatch(args.node_id):
        parser.error("--id debe tener 1-32 caracteres alfanuméricos, _ o -")
    try:
        if args.ping:
            peer = TcpClient().ping(parse_endpoint(args.ping), args.node_id)
            print(f"OK {args.node_id} -> {args.ping}: PONG de {peer}")
            return 0
        peers: Dict[str, Endpoint] = {}
        for text in args.peer:
            peer, endpoint = parse_peer(text)
            if peer in peers:
                raise ValueError(f"Peer repetido: {peer}")
            peers[peer] = endpoint
        if args.members is not None:
            members = set(args.members.split(","))
        else:
            members = set(peers) | {args.node_id}
        if args.inspect and args.data_dir is None:
            raise ValueError("--inspect requiere --data-dir")
        if args.data_dir is None:
            if peers:
                raise ValueError("--peer requiere --data-dir")
            return serve(args.node_id, args.host, args.port, members, RpcHandler())
        raise ValueError("El motor Raft Python se conecta al codec DM2 en el sub-paso I.3")
    except (OSError, ValueError) as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 1


def serve(node_id: str, host: str, port: int, members: Iterable[str], handler: RpcHandler,
          on_listen=None) -> int:
    dispatcher = Dispatcher(node_id, members, handler)
    with TcpServer(host, port, dispatcher) as server:
        print(f"[PYTHON {node_id}] LISTEN {host}:{server.port} protocol=DM1+DM2", flush=True)
        if on_listen is not None:
            on_listen()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
