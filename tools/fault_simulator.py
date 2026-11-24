"""Simulador de fallos, particiones de red y recuperación de log (Integrante 2)."""
from __future__ import annotations

import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
NODE_SCRIPT = ROOT / "nodes" / "python" / "node.py"
sys.path.insert(0, str(ROOT / "nodes" / "python"))

from node import encode_frame, recv_frame
from protocol_dm2 import build_dm2, parse_dm2

CLUSTER = {
    "n1": ("127.0.0.1", 9201),
    "n2": ("127.0.0.1", 9202),
    "n3": ("127.0.0.1", 9203),
}


def query_node(addr: Tuple[str, int], msg_type: str, payload: Dict[str, Any], timeout: float = 2.0) -> Optional[Dict[str, Any]]:
    try:
        with socket.create_connection(addr, timeout=timeout) as conn:
            conn.settimeout(timeout)
            conn.sendall(encode_frame(build_dm2(msg_type, "fault-sim", payload)))
            return parse_dm2(recv_frame(conn))
    except OSError:
        return None


def wait_for_leader(timeout: float = 5.0) -> Optional[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders = set()
        for nid, addr in CLUSTER.items():
            resp = query_node(addr, "NODE_STATUS", {}, timeout=0.4)
            if resp:
                m = resp["payload"]["metrics"]
                if not m.get("isolated") and m.get("state") == "LEADER":
                    leaders.add(nid)
        if len(leaders) == 1:
            return next(iter(leaders))
        time.sleep(0.1)
    return None


def run_fault_suite() -> int:
    test_data = ROOT / "data_fault_test"
    if test_data.exists():
        shutil.rmtree(test_data)

    peers_arg = ",".join(f"{nid}={h}:{p}" for nid, (h, p) in CLUSTER.items())
    procs: Dict[str, subprocess.Popen] = {}

    try:
        print("=== 1. Levantando clúster de 3 nodos (Raft) ===")
        for nid, (host, port) in CLUSTER.items():
            procs[nid] = subprocess.Popen(
                [
                    sys.executable,
                    str(NODE_SCRIPT),
                    "--id",
                    nid,
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--peers",
                    peers_arg,
                    "--algo",
                    "raft",
                    "--data-dir",
                    str(test_data),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        leader1 = wait_for_leader(6.0)
        assert leader1 is not None, "No se eligió líder inicial"
        print(f"[OK] Líder inicial elegido: {leader1}")

        # Enviar 5 mensajes iniciales
        for i in range(1, 6):
            res = query_node(
                CLUSTER[leader1],
                "CLIENT_COMMAND",
                {"command": f"msg-inicial-{i}", "requestId": uuid.uuid4().hex},
            )
            assert res and res["payload"].get("committed"), f"Fallo al comprometer msg {i}"
        print("[OK] 5 mensajes iniciales replicados y comprometidos")

        print(f"\n=== 2. Simulando partición de red del líder ({leader1}) ===")
        t_part = time.perf_counter()
        query_node(CLUSTER[leader1], "FAULT_CONTROL", {"isolated": True})

        leader2 = None
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            cand = wait_for_leader(0.5)
            if cand and cand != leader1:
                leader2 = cand
                break
        failover_ms = (time.perf_counter() - t_part) * 1000
        assert leader2 is not None, "El clúster no eligió nuevo líder tras aislar al anterior"
        print(f"[OK] Nuevo líder elegido tras partición: {leader2} en {failover_ms:.2f} ms")

        # Enviar 5 mensajes mientras el antiguo líder está aislado
        for i in range(6, 11):
            res = query_node(
                CLUSTER[leader2],
                "CLIENT_COMMAND",
                {"command": f"msg-durante-particion-{i}", "requestId": uuid.uuid4().hex},
            )
            assert res and res["payload"].get("committed"), f"Fallo en msg {i} bajo partición"
        print("[OK] 5 mensajes comprometidos por mayoría con 1 nodo aislado")

        print(f"\n=== 3. Sanando partición y verificando sincronización de {leader1} ===")
        t_heal = time.perf_counter()
        query_node(CLUSTER[leader1], "FAULT_CONTROL", {"isolated": False})

        synced = False
        while time.monotonic() - t_heal < 4.0:
            st = query_node(CLUSTER[leader1], "NODE_STATUS", {})
            if st and st["payload"]["metrics"]["commitIndex"] == 10:
                synced = True
                break
            time.sleep(0.1)
        recovery_ms = (time.perf_counter() - t_heal) * 1000
        assert synced, f"{leader1} no sincronizó las 10 entradas del log"
        print(f"[OK] Nodo {leader1} sincronizó las 10 entradas en {recovery_ms:.2f} ms")

        print(f"\n=== 4. Simulando caída abrupta (SIGKILL) y recuperación desde disco de {leader2} ===")
        procs[leader2].kill()
        procs[leader2].wait()

        leader3 = wait_for_leader(5.0)
        assert leader3 is not None and leader3 != leader2, "No hubo reelección tras caída del proceso"
        print(f"[OK] Nuevo líder tras caída abrupta: {leader3}")

        # Reiniciar nodo caído desde su persistencia en disco
        host, port = CLUSTER[leader2]
        procs[leader2] = subprocess.Popen(
            [
                sys.executable,
                str(NODE_SCRIPT),
                "--id",
                leader2,
                "--host",
                host,
                "--port",
                str(port),
                "--peers",
                peers_arg,
                "--algo",
                "raft",
                "--data-dir",
                str(test_data),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.8)
        st_recovered = query_node(CLUSTER[leader2], "NODE_STATUS", {})
        assert st_recovered and st_recovered["payload"]["metrics"]["logLength"] == 10
        print(f"[OK] Nodo {leader2} reiniciado y log de 10 entradas intacto en disco")

        print("\nPASS: Todas las pruebas de fallos, partición y recuperación superadas.")
        return 0
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        if test_data.exists():
            shutil.rmtree(test_data, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(run_fault_suite())