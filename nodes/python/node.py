"""Dog Messenger PC4: nodo TCP de conectividad (NO implementa Raft). SDK Python."""
from __future__ import annotations

import argparse
import re
import socket
import struct
import sys
import threading
import uuid

MAX_FRAME = 1024 * 1024
ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,32}$")
REQ_RE = re.compile(r"^[a-fA-F0-9]{32}$")


def encode_frame(body: str) -> bytes:
    payload = body.encode("utf-8")
    if not payload or len(payload) > MAX_FRAME:
        raise ValueError("Tamaño de trama fuera del límite")
    return struct.pack("!I", len(payload)) + payload


def recv_exact(conn: socket.socket, n: int) -> bytes:
    chunks = []
    while n:
        part = conn.recv(n)
        if not part:
            raise ConnectionError("Conexión cerrada antes de completar trama")
        chunks.append(part)
        n -= len(part)
    return b"".join(chunks)


def recv_frame(conn: socket.socket) -> str:
    size = struct.unpack("!I", recv_exact(conn, 4))[0]
    if size == 0 or size > MAX_FRAME:
        raise ValueError("Longitud de trama inválida")
    return recv_exact(conn, size).decode("utf-8", errors="strict")


def handle(conn: socket.socket, node_id: str) -> None:
    with conn:
        conn.settimeout(5)
        try:
            parts = recv_frame(conn).split("|")
            if (len(parts) != 4 or parts[0] != "DM1" or parts[1] != "PING"
                or not ID_RE.fullmatch(parts[2]) or not REQ_RE.fullmatch(parts[3])):
                # Este starter solo admite PING; Raft queda para etapas posteriores.
                conn.sendall(encode_frame(f"DM1|ERROR|{node_id}|00000000000000000000000000000000"))
                return
            conn.sendall(encode_frame(f"DM1|PONG|{node_id}|{parts[3]}"))
        except (OSError, ConnectionError, UnicodeError, ValueError):
            pass


def serve(node_id: str, host: str, port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(64)
        print(f"[PYTHON {node_id}] LISTEN {host}:{port} protocol=DM1", flush=True)
        while True:
            conn, _ = server.accept()
            threading.Thread(target=handle, args=(conn, node_id), daemon=True).start()


def ping(node_id: str, endpoint: str) -> int:
    host, separator, port_str = endpoint.rpartition(":")
    if not separator or not host:
        raise ValueError("Usa --ping IP:PUERTO")
    request_id = uuid.uuid4().hex
    with socket.create_connection((host, int(port_str)), timeout=5) as conn:
        conn.settimeout(5)
        conn.sendall(encode_frame(f"DM1|PING|{node_id}|{request_id}"))
        response = recv_frame(conn).split("|")
    if len(response) == 4 and response[0:2] == ["DM1", "PONG"] and ID_RE.fullmatch(response[2]) and response[3] == request_id:
        print(f"OK {node_id} -> {endpoint}: PONG de {response[2]}")
        return 0
    print(f"ERROR respuesta inesperada: {response}", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Nodo TCP base Dog Messenger; NO es Raft")
    parser.add_argument("--id", required=True, dest="node_id")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9102)
    parser.add_argument("--ping", metavar="IP:PUERTO")
    args = parser.parse_args()
    if not ID_RE.fullmatch(args.node_id):
        parser.error("--id debe tener 1-32 caracteres alfanuméricos, _ o -")
    try:
        if args.ping:
            return ping(args.node_id, args.ping)
        serve(args.node_id, args.host, args.port)
        return 0
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
