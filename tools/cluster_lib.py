"""Utilidades para levantar clústeres locales Java/Python y conversar con ellos en DM2 binario.

Solo biblioteca estándar y pathlib: funciona igual en Ubuntu y Windows. El estado de cada nodo
se obtiene de sus logs (formato común "[RAFT id] role=... term=...") y de --inspect.
"""
from __future__ import annotations

import json
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nodes" / "python"))
import dm2_codec as dm2  # noqa: E402
import node as dm_node  # noqa: E402

HOST = "127.0.0.1"
ROLE_LINE = re.compile(r"\[RAFT (\S+)\] role=(\w+) term=(\d+) leader=(\S+) event=(\S+) commitIndex=(\d+)")
COMMIT = re.compile(r"\[RAFT \S+\] .*?commitIndex=(\d+)")


class StepFailed(Exception):
    pass


def is_listening(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=0.2):
            return True
    except OSError:
        return False


def java_command() -> List[str]:
    return ["java", "-cp", str(ROOT / "build" / "java"), "dogmessenger.NodeMain"]


def python_command() -> List[str]:
    return [sys.executable, str(ROOT / "nodes" / "python" / "node.py")]


class ClusterNode:
    def __init__(self, node_id: str, lang: str, port: int, workdir: Path) -> None:
        self.node_id, self.lang, self.port = node_id, lang, port
        self.data_dir = workdir / f"data-{node_id}"
        self.log_path = workdir / f"{node_id}-{lang}.log"
        self.peers: Dict[str, int] = {}
        self.proc: Optional[subprocess.Popen] = None
        self.isolated = False

    @property
    def base(self) -> List[str]:
        return java_command() if self.lang == "java" else python_command()

    def args(self) -> List[str]:
        args = ["--id", self.node_id, "--host", HOST, "--port", str(self.port), "--data-dir", str(self.data_dir)]
        for peer, port in sorted(self.peers.items()):
            args += ["--peer", f"{peer}={HOST}:{port}"]
        return args

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self, wait: float = 15.0) -> None:
        if is_listening(self.port):
            raise StepFailed(f"Puerto {self.port} ocupado; cierra otras instancias")
        with self.log_path.open("ab") as log:
            log.write(f"=== start {time.strftime('%H:%M:%S')} ===\n".encode())
            self.proc = subprocess.Popen(self.base + self.args(), stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise StepFailed(f"{self.node_id} terminó al iniciar (exit={self.proc.returncode}); ver {self.log_path}")
            if is_listening(self.port):
                return
            time.sleep(0.05)
        raise StepFailed(f"{self.node_id} no empezó a escuchar en {self.port}")

    def kill(self) -> None:
        """Caída abrupta: SIGKILL en POSIX, TerminateProcess en Windows."""
        if self.alive:
            self.proc.kill()
        if self.proc is not None:
            self.proc.wait()

    def stop(self) -> None:
        if self.alive:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def set_isolated(self, isolated: bool) -> None:
        """Partición simulada (solo nodos Python): <data-dir>/fault.json."""
        if self.lang != "python":
            raise ValueError("El aislamiento por archivo solo existe en el nodo Python")
        (self.data_dir / "fault.json").write_text(json.dumps({"isolated": isolated}), encoding="utf-8")
        self.isolated = isolated

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def state(self) -> Dict[str, object]:
        text = self.log_text()
        roles = [m for m in ROLE_LINE.finditer(text) if m.group(1) == self.node_id]
        commits = [int(m.group(1)) for m in COMMIT.finditer(text)]
        last = roles[-1] if roles else None
        return {
            "role": last.group(2) if last else "UNKNOWN",
            "term": int(last.group(3)) if last else 0,
            "commit": max(commits) if commits else 0,
        }

    def inspect(self) -> List[str]:
        """Estado persistido. Java bloquea su directorio: inspeccionar con el nodo detenido."""
        result = subprocess.run(self.base + self.args() + ["--inspect"], capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise StepFailed(f"--inspect de {self.node_id} falló: {result.stdout}{result.stderr}")
        return result.stdout.splitlines()


def committed_entries(inspect_lines: Iterable[str]) -> List[str]:
    """Entradas COMMITTED de --inspect (índice, término, tipo, cliente, requestId)."""
    return [line for line in inspect_lines if line.startswith("index=") and "status=COMMITTED" in line]


class Cluster:
    def __init__(self, specs: Iterable[Tuple[str, str, int]], workdir: Path) -> None:
        self.workdir = workdir
        self.nodes: Dict[str, ClusterNode] = {nid: ClusterNode(nid, lang, port, workdir) for nid, lang, port in specs}
        for current in self.nodes.values():
            current.peers = {nid: n.port for nid, n in self.nodes.items() if nid != current.node_id}
        self.client = dm_node.TcpClient(timeout=2.0)

    def stop_all(self) -> None:
        for current in self.nodes.values():
            current.stop()

    def find_leader(self, timeout: float = 10.0, exclude: Iterable[str] = ()) -> Optional[ClusterNode]:
        """Líder único de mayor término entre los nodos vivos y no aislados (según sus logs)."""
        excluded = set(exclude)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            candidates = [(n.state()["term"], n) for n in self.nodes.values()
                          if n.alive and not n.isolated and n.node_id not in excluded and n.state()["role"] == "LEADER"]
            if candidates:
                top = max(term for term, _ in candidates)
                leaders = [n for term, n in candidates if term == top]
                if len(leaders) == 1:
                    return leaders[0]
            time.sleep(0.05)
        return None

    def send_command(self, data: bytes, client_id: str = "client1", timeout: float = 15.0) -> Tuple[int, str]:
        """CLIENT_COMMAND siguiendo NOT_LEADER. Los reintentos reutilizan (clientId, requestId) y bytes."""
        request = dm2.Message(dm2.Header(dm2.RAFT, dm2.CLIENT_COMMAND, dm_node.new_request_id(), client_id),
                              dm2.ClientCommand(data))
        order = list(self.nodes)
        target: Optional[str] = None
        attempt = 0
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if target is None or not self.nodes[target].alive or self.nodes[target].isolated:
                alive = [nid for nid in order if self.nodes[nid].alive and not self.nodes[nid].isolated]
                if not alive:
                    break
                target = alive[attempt % len(alive)]
                attempt += 1
            try:
                payload = self.client.call((HOST, self.nodes[target].port), target, request).payload
            except OSError:
                payload = None
            if isinstance(payload, dm2.ClientResult):
                if payload.status == dm2.COMMITTED:
                    return payload.log_index, target
                if payload.status == dm2.REQUEST_ID_CONFLICT:
                    raise StepFailed(f"REQUEST_ID_CONFLICT inesperado en {target}")
                if payload.status == dm2.NOT_LEADER:
                    hint = payload.leader_hint
                    target = hint if hint and hint != target else None
                    if target is not None:
                        continue
                # RETRY: mismo destino, mismo requestId.
            else:
                target = None  # Sin respuesta o ERROR: probar otra réplica.
            time.sleep(0.1)
        raise StepFailed("El comando no se confirmó dentro del plazo")

    def wait_commit(self, nodes: Iterable[ClusterNode], index: int, timeout: float = 10.0) -> None:
        pending = list(nodes)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pending = [n for n in pending if int(n.state()["commit"]) < index]
            if not pending:
                return
            time.sleep(0.05)
        raise StepFailed(f"No alcanzaron commitIndex {index}: {[n.node_id for n in pending]}")


class Report:
    """Imprime PASS/FAIL por paso y acumula el veredicto."""

    def __init__(self) -> None:
        self.failed = 0
        self.passed = 0

    def step(self, name: str, func):
        start = time.perf_counter()
        try:
            detail = func()
        except Exception as error:  # noqa: BLE001 - el reporte debe seguir mostrando el fallo
            self.failed += 1
            print(f"[FAIL] {name}: {error}", flush=True)
            raise
        self.passed += 1
        elapsed = (time.perf_counter() - start) * 1000
        print(f"[PASS] {name}{': ' + detail if detail else ''} ({elapsed:.0f} ms)", flush=True)
        return detail
