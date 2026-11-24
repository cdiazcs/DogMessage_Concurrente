"""Dog Messenger PC4: Nodo Python con soporte TCP DM1/DM2, Raft y Segundo Consenso (2PC)."""
from __future__ import annotations

import argparse
import json
import re
import socket
import struct
import sys
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Permitir importación tanto si se ejecuta desde raíz como desde nodes/python
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from protocol_dm2 import build_dm2, parse_dm2
from raft_engine import RaftEngine
from secondary_engine import TwoPhaseCommitEngine

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


def send_dm2_rpc(
    peer_id: str,
    addr: Tuple[str, int],
    framed_json: str,
    timeout: float = 0.5,
) -> Optional[Dict[str, Any]]:
    try:
        with socket.create_connection(addr, timeout=timeout) as conn:
            conn.settimeout(timeout)
            conn.sendall(encode_frame(framed_json))
            raw_resp = recv_frame(conn)
            return parse_dm2(raw_resp)
    except (OSError, ConnectionError, ValueError, json.JSONDecodeError):
        return None


def parse_peers(peers_str: str, self_id: str) -> Dict[str, Tuple[str, int]]:
    peers: Dict[str, Tuple[str, int]] = {}
    if not peers_str.strip():
        return peers
    for item in peers_str.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item or ":" not in item:
            raise ValueError(f"Formato de peer inválido '{item}'. Usa id=host:puerto")
        pid, endpoint = item.split("=", 1)
        pid = pid.strip()
        if pid == self_id:
            continue
        host, _, port_str = endpoint.rpartition(":")
        peers[pid] = (host.strip(), int(port_str))
    return peers


def dispatch_dm2(msg: Dict[str, Any], node_id: str, engine: Any, algo: str) -> str:
    msg_type = msg["type"]
    payload = msg["payload"]

    if msg_type == "NODE_STATUS":
        return build_dm2(
            "NODE_STATUS_RESPONSE",
            node_id,
            {
                "metrics": engine.metrics(),
                "committedCount": len(engine.committedEntries()),
                "entries": engine.committedEntries()[-10:],
            },
        )

    if msg_type == "FAULT_CONTROL":
        state = engine.set_fault_state(
            isolated=payload.get("isolated"),
            delay_ms=payload.get("delay_ms"),
        )
        return build_dm2("FAULT_CONTROL_RESPONSE", node_id, {"ok": True, **state})

    if msg_type == "CLIENT_COMMAND":
        command = str(payload.get("command", ""))
        req_id = str(payload.get("requestId") or uuid.uuid4().hex)
        timeout = float(payload.get("timeout", 3.0))
        res = engine.propose(command, req_id, timeout=timeout)
        return build_dm2("CLIENT_RESULT", node_id, res)

    if algo == "raft":
        if msg_type == "RAFT_REQUEST_VOTE":
            res = engine.handle_request_vote(payload)
            return build_dm2("RAFT_REQUEST_VOTE_RESPONSE", node_id, res)
        if msg_type == "RAFT_APPEND_ENTRIES":
            res = engine.handle_append_entries(payload)
            return build_dm2("RAFT_APPEND_ENTRIES_RESPONSE", node_id, res)
    else:
        if msg_type == "CONSENSUS2_PREPARE":
            res = engine.handle_prepare(payload)
            return build_dm2("CONSENSUS2_PREPARE_RESPONSE", node_id, res)
        if msg_type == "CONSENSUS2_COMMIT":
            res = engine.handle_commit(payload)
            return build_dm2("CONSENSUS2_COMMIT_RESPONSE", node_id, res)

    return build_dm2("CLIENT_RESULT", node_id, {"ok": False, "error": f"UNHANDLED_{msg_type}"})


def handle(conn: socket.socket, node_id: str, engine: Any, algo: str) -> None:
    with conn:
        conn.settimeout(5)
        try:
            raw = recv_frame(conn)
            # Soporte retrocompatible DM1 (PING/PONG)
            if raw.startswith("DM1|"):
                parts = raw.split("|")
                if (
                    len(parts) != 4
                    or parts[1] != "PING"
                    or not ID_RE.fullmatch(parts[2])
                    or not REQ_RE.fullmatch(parts[3])
                ):
                    conn.sendall(encode_frame(f"DM1|ERROR|{node_id}|00000000000000000000000000000000"))
                    return
                conn.sendall(encode_frame(f"DM1|PONG|{node_id}|{parts[3]}"))
                return

            # Protocolo DM2 (Consenso, Cliente, Métricas y Control de Fallos)
            msg = parse_dm2(raw)
            response_body = dispatch_dm2(msg, node_id, engine, algo)
            conn.sendall(encode_frame(response_body))
        except (OSError, ConnectionError, UnicodeError, ValueError, json.JSONDecodeError):
            try:
                conn.sendall(encode_frame(f"DM1|ERROR|{node_id}|00000000000000000000000000000000"))
            except OSError:
                pass


def serve(node_id: str, host: str, port: int, peers: Dict[str, Tuple[str, int]], algo: str, data_dir: str) -> None:
    if algo == "raft":
        engine: Any = RaftEngine(node_id, peers, send_dm2_rpc, data_dir=data_dir)
    else:
        engine = TwoPhaseCommitEngine(node_id, peers, send_dm2_rpc, data_dir=data_dir)

    engine.start()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((host, port))
            server.listen(128)
            server.settimeout(0.5)  # Permite salida limpia con Ctrl+C en Windows
            print(
                f"[PYTHON {node_id}] LISTEN {host}:{port} protocol=DM1/DM2 algo={algo.upper()} peers={list(peers.keys())}",
                flush=True,
            )
            while True:
                try:
                    conn, _ = server.accept()
                    threading.Thread(
                        target=handle, args=(conn, node_id, engine, algo), daemon=True
                    ).start()
                except socket.timeout:
                    continue
    except KeyboardInterrupt:
        print(f"\n[PYTHON {node_id}] Deteniendo nodo limpiamente...", flush=True)
    finally:
        engine.stop()


def ping(node_id: str, endpoint: str) -> int:
    host, separator, port_str = endpoint.rpartition(":")
    if not separator or not host:
        raise ValueError("Usa --ping IP:PUERTO")
    request_id = uuid.uuid4().hex
    with socket.create_connection((host, int(port_str)), timeout=5) as conn:
        conn.settimeout(5)
        conn.sendall(encode_frame(f"DM1|PING|{node_id}|{request_id}"))
        response = recv_frame(conn).split("|")
    if (
        len(response) == 4
        and response[0:2] == ["DM1", "PONG"]
        and ID_RE.fullmatch(response[2])
        and response[3] == request_id
    ):
        print(f"OK {node_id} -> {endpoint}: PONG de {response[2]}")
        return 0
    print(f"ERROR respuesta inesperada: {response}", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Nodo Dog Messenger Python (DM1 + DM2 + Raft/2PC)")
    parser.add_argument("--id", required=True, dest="node_id")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9102)
    parser.add_argument("--peers", default="", help="Lista de nodos: n1=127.0.0.1:9101,n3=127.0.0.1:9103")
    parser.add_argument("--algo", choices=["raft", "2pc"], default="raft", help="Algoritmo de consenso")
    parser.add_argument("--data-dir", default="data", help="Carpeta de persistencia del log")
    parser.add_argument("--ping", metavar="IP:PUERTO")
    args = parser.parse_args()

    if not ID_RE.fullmatch(args.node_id):
        parser.error("--id debe tener 1-32 caracteres alfanuméricos, _ o -")
    try:
        if args.ping:
            return ping(args.node_id, args.ping)
        peers = parse_peers(args.peers, args.node_id)
        serve(args.node_id, args.host, args.port, peers, args.algo, args.data_dir)
        return 0
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())