"""Simulador de fallos, particiones de red y recuperación de log (Integrante 2), sobre DM2 binario.

Escenarios originales conservados con 3 nodos Python:
  1. Clúster Raft y 5 comandos comprometidos.
  2. Partición del líder (aislamiento por <data-dir>/fault.json), reelección y 5 comandos.
  3. Sanar la partición: el antiguo líder sincroniza las 10 entradas.
  4. Caída abrupta (kill) del nuevo líder, reelección, reinicio desde disco y logs idénticos.

Uso:  Ubuntu: python3 tools/fault_simulator.py      Windows: py -3 tools\\fault_simulator.py
"""
from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cluster_lib import Cluster, Report, StepFailed, committed_entries  # noqa: E402

PORTS = {"n1": 9201, "n2": 9202, "n3": 9203}


def run_fault_suite(workdir: Path) -> int:
    cluster = Cluster([(nid, "python", port) for nid, port in PORTS.items()], workdir)
    report = Report()
    sent = 0
    state = {}

    def send(count: int, label: str) -> str:
        nonlocal sent
        last = 0
        for _ in range(count):
            sent += 1
            last, _leader = cluster.send_command(f"{label}-{sent}".encode())
        return f"{count} comandos COMMITTED, último índice {last}"

    def step1():
        for current in cluster.nodes.values():
            current.start()
        leader = cluster.find_leader(10)
        if leader is None:
            raise StepFailed("No se eligió líder inicial")
        state["leader1"] = leader
        return f"líder {leader.node_id} término {leader.state()['term']}; " + send(5, "msg-inicial")

    def step2():
        leader1 = state["leader1"]
        start = time.perf_counter()
        leader1.set_isolated(True)
        leader2 = cluster.find_leader(10, exclude=[leader1.node_id])
        if leader2 is None:
            raise StepFailed("El clúster no eligió nuevo líder tras aislar al anterior")
        state["leader2"] = leader2
        failover = (time.perf_counter() - start) * 1000
        return (f"{leader1.node_id} aislado; nuevo líder {leader2.node_id} en {failover:.0f} ms; "
                + send(5, "msg-durante-particion"))

    def step3():
        leader1, leader2 = state["leader1"], state["leader2"]
        start = time.perf_counter()
        leader1.set_isolated(False)
        cluster.wait_commit([leader1], int(leader2.state()["commit"]), timeout=10)
        return f"{leader1.node_id} sincronizó commitIndex {leader1.state()['commit']} en " \
               f"{(time.perf_counter() - start) * 1000:.0f} ms"

    def step4():
        leader2 = state["leader2"]
        leader2.kill()
        leader3 = cluster.find_leader(10, exclude=[leader2.node_id])
        if leader3 is None:
            raise StepFailed("No hubo reelección tras la caída del proceso")
        before = int(leader3.state()["commit"])
        recovered = leader2.inspect()  # El nodo Python permite inspeccionar sin bloquear el directorio.
        if len(committed_entries(recovered)) < 10:
            raise StepFailed(f"{leader2.node_id} perdió entradas comprometidas en disco")
        leader2.start()
        detail = send(2, "msg-tras-reinicio")
        target = max(before, int(leader3.state()["commit"]))
        cluster.wait_commit(cluster.nodes.values(), target, timeout=10)
        cluster.stop_all()
        logs = {nid: committed_entries(n.inspect()) for nid, n in cluster.nodes.items()}
        reference = logs[leader3.node_id]
        if len(reference) < 12 or any(entries != reference for entries in logs.values()):
            raise StepFailed(f"Logs comprometidos distintos: { {k: len(v) for k, v in logs.items()} }")
        return (f"{leader2.node_id} caído y reiniciado; nuevo líder {leader3.node_id}; {detail}; "
                f"{len(reference)} entradas comprometidas idénticas en los 3 nodos")

    try:
        report.step("1. Clúster de 3 nodos Python y 5 mensajes iniciales", step1)
        report.step("2. Partición del líder y reelección", step2)
        report.step("3. Sanar partición y sincronizar", step3)
        report.step("4. Caída abrupta, recuperación desde disco y logs idénticos", step4)
    except Exception:  # noqa: BLE001 - el detalle ya se imprimió
        pass
    finally:
        cluster.stop_all()
    verdict = "PASS" if report.failed == 0 and report.passed == 4 else "FAIL"
    print(f"\n{verdict}: {report.passed}/4 escenarios de fallo. Logs en {workdir}")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workdir", type=Path, help="Directorio para datos y logs (por defecto, uno temporal)")
    args = parser.parse_args()
    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="dm-fault-"))
    workdir.mkdir(parents=True, exist_ok=True)
    sys.exit(run_fault_suite(workdir))
