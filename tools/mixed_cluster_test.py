"""Clúster Raft mixto Java + Python en DM2 binario (fase I).

n1 Java + n2 Python + n3 Python en 127.0.0.1:19101-19103 con --data-dir temporales.
Se ejecutan dos rondas: con líder inicial Java y con líder inicial Python. Cada ronda:
  1. levanta los nodos forzando el lenguaje del líder inicial (ventaja de término);
  2. espera un líder y reporta id, lenguaje y término;
  3. envía 20 CLIENT_COMMAND concurrentes siguiendo NOT_LEADER y verifica COMMITTED;
     3b. verifica que los seguidores del otro lenguaje avanzan su commitIndex;
  4. mata al líder, verifica la reelección y confirma 10 comandos más;
  5. revive el nodo caído y compara con --inspect (Java y Python) que las entradas
     comprometidas son idénticas: índice, término, tipo, clientId, clientRequestId y command.
Al final revisa los logs de los nodos en busca de tracebacks, ERROR o respuestas descartadas.

Uso:  Ubuntu: python3 tools/mixed_cluster_test.py     Windows: py -3 tools\\mixed_cluster_test.py
"""
from __future__ import annotations

import argparse
import re
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_all import build_java  # noqa: E402
from cluster_lib import Cluster, ClusterNode, Report, StepFailed, committed_entries  # noqa: E402

SPECS = [("n1", "java", 19101), ("n2", "python", 19102), ("n3", "python", 19103)]
FIRST_BY_LANG = {"java": "n1", "python": "n2"}
PROBLEMS = re.compile(r"Traceback|\bERROR\b|Exception|rpc-discarded|rpc-error|stopped:")
REPLICATED = re.compile(r"\[RAFT \S+\] peer=(\S+) term=\d+ nextIndex=\d+ matchIndex=(\d+) event=replicated")
ENTRY_FIELDS = re.compile(r"index=(\d+) term=(\d+) type=(\S+) status=COMMITTED client=(\S+) requestId=(\S+) "
                          r"command=(\S+)$")


def stable_leader(cluster: Cluster, timeout: float = 15.0, exclude=()) -> Optional[ClusterNode]:
    """Un líder que sigue siéndolo, con el mismo término, medio segundo después."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        first = cluster.find_leader(max(0.1, deadline - time.monotonic()), exclude)
        if first is None:
            return None
        term = first.state()["term"]
        time.sleep(0.5)
        again = cluster.find_leader(1.0, exclude)
        if again is first and again.state()["term"] == term:
            return first
    return None


def describe(n: ClusterNode) -> str:
    return f"{n.node_id} ({n.lang}) término {n.state()['term']}"


class Round:
    def __init__(self, target_lang: str, workdir: Path, report: Report) -> None:
        self.target_lang = target_lang
        self.workdir = workdir
        self.report = report
        self.cluster: Optional[Cluster] = None
        self.leader: Optional[ClusterNode] = None
        self.leader_terms: set[int] = set()
        self.sent: Dict[str, bytes] = {}  # comandos confirmados, indexados por su hex
        self.lock = threading.Lock()

    def tag(self, text: str) -> str:
        return f"[líder inicial {self.target_lang}] {text}"

    # --- pasos ---------------------------------------------------------------------------
    def start_forced(self) -> str:
        first_id = FIRST_BY_LANG[self.target_lang]
        for attempt in range(1, 6):
            cluster = Cluster(SPECS, self.workdir / f"intento-{attempt}")
            cluster.workdir.mkdir(parents=True, exist_ok=True)
            self.cluster = cluster
            first = cluster.nodes[first_id]
            first.start()
            deadline = time.monotonic() + 10
            while first.state()["term"] < 3 and time.monotonic() < deadline:
                time.sleep(0.05)  # Ventaja de término: los demás arrancan con término 0.
            for node in cluster.nodes.values():
                if node is not first:
                    node.start()
            leader = stable_leader(cluster)
            if leader is not None and leader.lang == self.target_lang:
                self.leader = leader
                self.leader_terms.add(int(leader.state()["term"]))
                return f"3 nodos arriba; líder {self.target_lang} en el intento {attempt}"
            cluster.stop_all()
        raise StepFailed(f"No se logró un líder inicial {self.target_lang} en 5 intentos")

    def report_leader(self) -> str:
        return f"líder {describe(self.leader)}"

    def send_many(self, count: int, label: str, workers: int = 4) -> List[int]:
        indexes: List[int] = []
        errors: List[Exception] = []

        def worker(w: int) -> None:
            for i in range(w, count, workers):
                data = f"{label}-{i}".encode() + b"\x00\xff"  # bytes opacos, no solo texto
                try:
                    index, _ = self.cluster.send_command(data, client_id=f"{label}-c{w}")
                except Exception as error:  # noqa: BLE001
                    errors.append(error)
                    return
                with self.lock:
                    indexes.append(index)
                    self.sent[data.hex()] = data

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise StepFailed(f"{len(errors)} comandos fallaron: {errors[0]}")
        if len(set(indexes)) != count:
            raise StepFailed(f"Índices COMMITTED repetidos o faltantes: {sorted(indexes)}")
        return indexes

    def commands_20(self) -> str:
        indexes = self.send_many(20, f"{self.target_lang}-fase1")
        return f"20/20 COMMITTED vía {self.leader.node_id}, índices {min(indexes)}..{max(indexes)}"

    def cross_language(self) -> str:
        leader = self.leader
        target = int(leader.state()["commit"])
        others = [n for n in self.cluster.nodes.values() if n is not leader]
        self.cluster.wait_commit(others, target, timeout=10)
        foreign = [n for n in others if n.lang != leader.lang]
        if not foreign:
            raise StepFailed("No hay seguidores de otro lenguaje")
        detail = ", ".join(f"{n.node_id} ({n.lang}) commitIndex {n.state()['commit']}" for n in foreign)
        batches = ""
        if leader.lang == "java":
            jumps = self.max_batches(leader)
            batches = "; lote máximo aceptado " + ", ".join(f"{p}={j}" for p, j in sorted(jumps.items()))
        return f"seguidores {self.other_lang(leader)} bajo líder {leader.lang}: {detail}{batches}"

    @staticmethod
    def other_lang(n: ClusterNode) -> str:
        return "python" if n.lang == "java" else "java"

    @staticmethod
    def max_batches(java_leader: ClusterNode) -> Dict[str, int]:
        """Mayor avance de matchIndex en una sola respuesta, según el log del líder Java."""
        last: Dict[str, int] = {}
        jumps: Dict[str, int] = {}
        for peer, match in REPLICATED.findall(java_leader.log_text()):
            match = int(match)
            jumps[peer] = max(jumps.get(peer, 0), match - last.get(peer, 0))
            last[peer] = match
        return jumps

    def kill_leader(self) -> str:
        old = self.leader
        old.kill()
        self.killed = old
        new = stable_leader(self.cluster, exclude=[old.node_id])
        if new is None:
            raise StepFailed("No hubo reelección tras matar al líder")
        self.leader = new
        self.leader_terms.add(int(new.state()["term"]))
        indexes = self.send_many(10, f"{self.target_lang}-fase2")
        alive = [n for n in self.cluster.nodes.values() if n.alive]
        self.cluster.wait_commit(alive, max(indexes), timeout=10)
        return f"{old.node_id} ({old.lang}) muerto; nuevo líder {describe(new)}; 10/10 COMMITTED hasta {max(indexes)}"

    def revive_and_compare(self) -> str:
        revived = self.killed
        revived.start()
        target = int(self.leader.state()["commit"])
        self.cluster.wait_commit([revived], target, timeout=15)
        relation = f"{revived.node_id} ({revived.lang}) revivido y al día bajo líder {self.leader.lang} " \
                   f"(commitIndex {revived.state()['commit']})"
        self.cluster.stop_all()  # Java bloquea su directorio: --inspect con los nodos detenidos.
        logs = {nid: [ENTRY_FIELDS.search(line).groups() for line in committed_entries(n.inspect())]
                for nid, n in self.cluster.nodes.items()}
        reference = logs[self.leader.node_id]
        for nid, entries in logs.items():
            if entries != reference:
                diff = next((i for i, (a, b) in enumerate(zip(entries, reference)) if a != b), min(len(entries),
                                                                                                    len(reference)))
                raise StepFailed(f"{nid} difiere del líder desde la entrada {diff + 1} "
                                 f"({len(entries)} vs {len(reference)} comprometidas)")
        commands = [entry for entry in reference if entry[2] == "COMMAND"]
        if sorted(e[5] for e in commands) != sorted(self.sent):
            raise StepFailed("Los comandos comprometidos no coinciden con los 30 enviados")
        keys = {(e[3], e[4]) for e in commands}
        if len(keys) != len(commands):
            raise StepFailed("Hay (clientId, clientRequestId) repetidos en el log comprometido")
        no_ops = [entry for entry in reference if entry[2] == "NO_OP"]
        for term in self.leader_terms:
            if sum(int(entry[1]) == term for entry in no_ops) != 1:
                raise StepFailed(f"Se esperaba un NO_OP comprometido para el liderazgo del término {term}")
        return (f"{relation}; --inspect Java y Python: {len(reference)} entradas comprometidas idénticas "
                f"({len(commands)} COMMAND = los 30 enviados, {len(no_ops)} NO_OP)")

    def run(self) -> None:
        steps = [
            ("1. Levantar n1 Java + n2/n3 Python", self.start_forced),
            ("2. Líder elegido", self.report_leader),
            ("3. 20 CLIENT_COMMAND siguiendo NOT_LEADER", self.commands_20),
            ("3b. Replicación entre lenguajes", self.cross_language),
            ("4. Matar al líder, reelección y 10 comandos", self.kill_leader),
            ("5. Revivir y comparar --inspect", self.revive_and_compare),
        ]
        try:
            for name, func in steps:
                self.report.step(self.tag(name), func)
        finally:
            if self.cluster is not None:
                self.cluster.stop_all()


def scan_logs(workdir: Path) -> str:
    findings = []
    for log in sorted(workdir.rglob("*.log")):
        for number, line in enumerate(log.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if PROBLEMS.search(line):
                findings.append(f"{log.relative_to(workdir)}:{number}: {line.strip()}")
    if findings:
        raise StepFailed(f"{len(findings)} líneas problemáticas:\n  " + "\n  ".join(findings[:20]))
    return f"{len(list(workdir.rglob('*.log')))} logs sin Traceback, ERROR ni respuestas descartadas"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workdir", type=Path, help="Directorio para datos y logs (por defecto, uno temporal)")
    args = parser.parse_args()
    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="dm-mixed-"))
    workdir.mkdir(parents=True, exist_ok=True)
    report = Report()
    try:
        report.step("0. Compilar Java", lambda: build_java() or "build/java")
        for lang in ("java", "python"):
            Round(lang, workdir / f"lider-{lang}", report).run()
        report.step("6. Revisión de logs de los nodos", lambda: scan_logs(workdir))
    except Exception:  # noqa: BLE001 - el paso fallido ya se imprimió
        pass
    verdict = "PASS" if report.failed == 0 and report.passed == 14 else "FAIL"
    print(f"\n{verdict}: {report.passed}/14 pasos. Logs en {workdir}")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
