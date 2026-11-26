"""Banco de 2PC por TCP en loopback con tres procesos y cortes DM2 observables."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import csv
import hashlib
import json
from pathlib import Path
import platform
import socket
import subprocess
import tempfile
import threading
import time
import uuid

from build_all import build_java, build_kotlin
from cluster_lib import Cluster, HOST, StepFailed, dm2, dm_node
from trilingual_cluster_test import IDS, LANGS, Link, eventually, require, reserve_ports
import two_pc_wire as w


class Bank:
    def __init__(self, path, coordinator):
        self.path = path
        path.mkdir(parents=True)
        self.cluster = Cluster(zip(IDS, LANGS, reserve_ports(3)), path)
        self.nodes = self.cluster.nodes
        self.coordinator = coordinator
        self.links = {}
        self.hooks = []
        self.trace = []
        self.lock = threading.Lock()
        self.client = dm_node.TcpClient(1.5)
        try:
            for node in self.nodes.values():
                node.coordinator = coordinator
                for peer in self.nodes:
                    if peer == node.node_id:
                        continue
                    src, dst = node.node_id, peer
                    link = Link(0, self.nodes[dst].port,
                                lambda body, a=src, b=dst: self.observe(a, b, "request", body),
                                lambda body, a=src, b=dst: self.observe(a, b, "response", body))
                    while link.port in {n.port for n in self.nodes.values()}:
                        link.close()
                        link = Link(0, self.nodes[dst].port,
                                    lambda body, a=src, b=dst: self.observe(a, b, "request", body),
                                    lambda body, a=src, b=dst: self.observe(a, b, "response", body))
                    self.links[src, dst] = link
                    node.peers[dst] = link.port
        except BaseException:
            self.close()
            raise

    def observe(self, src, dst, stage, body):
        try:
            _, kind, _, _, _ = w.header(body)
        except ValueError:
            return False
        with self.lock:
            self.trace.append((src, dst, stage, kind, body))
            hooks = self.hooks[:]
        for hook in hooks:
            if not hook(src, dst, stage, kind):
                return False
        return True

    def pause(self, stage, kind, src=None, dst=None, occurrence=1):
        arrived, released = threading.Event(), threading.Event()
        matches = []
        def gate(a, b, current, message):
            if ((stage, kind) == (current, message) and (src is None or a == src)
                    and (dst is None or b == dst)):
                with self.lock:
                    matches.append((a, b))
                    hold = len(matches) == occurrence
                if hold:
                    arrived.set()
                    released.wait(2)
                    return False
            return True
        with self.lock:
            self.hooks.append(gate)
        return arrived, released, matches

    def mute_queries(self):
        with self.lock:
            self.hooks.append(lambda a, b, phase, kind: kind != w.QUERY)

    def start(self):
        for node in self.nodes.values():
            node.start()

    def close(self):
        try:
            self.cluster.stop_all()
        finally:
            for link in self.links.values():
                link.close()
            events = [{"source": a, "destination": b, "phase": stage, "type": kind}
                      for a, b, stage, kind, _ in self.trace]
            (self.path / "wire-events.json").write_text(json.dumps(events, indent=2), encoding="utf-8")

    def send(self, target, kind, token, sender, payload):
        body = self.client.exchange((HOST, self.nodes[target].port), w.encode(kind, token, sender, payload))
        return w.correlate(body, target, token, kind)

    def submit(self, token, data, client="bench", target=None):
        return self.send(target or self.coordinator, w.SUBMIT, token, client, w.command_bytes(data))

    def final(self, token, data, status=w.COMMITTED, client="bench"):
        def attempt():
            try:
                result = self.submit(token, data, client)
            except OSError:
                return None
            if result[0] == w.PENDING:
                return None
            require(result[0] == status, f"Resultado inesperado: {result}, se esperaba {status}")
            return result
        return eventually(attempt, "2PC no alcanzó decisión confirmada", timeout=18)

    def decision(self, timeout=8):
        node = self.nodes[self.coordinator]
        return eventually(lambda: " decision=" in node.log_text(), "No se observó decisión durable", timeout)

    def compare(self, expected_committed, expected_aborted=0):
        self.cluster.stop_all()
        reference = None
        hashes = {}
        for node in self.nodes.values():
            lines = node.inspect()
            (self.path / f"{node.node_id}.inspect").write_text("\n".join(lines) + "\n", encoding="utf-8")
            rows = [line for line in lines if line.startswith("sequence=")]
            require(not any("PREPARED" in s or "STARTED" in s for s in rows), f"Pendiente en {node.node_id}")
            committed = [line for line in rows if "status=COMMIT" in line]
            aborted = [line for line in rows if "status=ABORT" in line]
            require(len(committed) == expected_committed and len(aborted) == expected_aborted, str(rows))
            if reference is None:
                reference = rows
            require(reference == rows, f"Estado diferente en {node.node_id}")
            canonical = ("\n".join(committed) + "\n").encode("utf-8")
            (self.path / f"{node.node_id}.committed.json").write_text(json.dumps(committed), encoding="utf-8")
            hashes[node.node_id] = hashlib.sha256(canonical).hexdigest()
        return {"commit": expected_committed, "abort": expected_aborted, "pending": 0, "hashes": hashes}


def normal(bank, count=20, concurrent=False):
    def send(worker):
        output = []
        for i in range(count):
            token = uuid.uuid4().bytes
            data = bytes(range(256)) + bytes([worker, i])
            result = bank.final(token, data, client=f"c{worker}")
            output.append((token, data, result[1], f"c{worker}"))
        return output
    bank.start()
    participant = next(id for id in IDS if id != bank.coordinator)
    require(bank.submit(uuid.uuid4().bytes, b"x", target=participant)[0] == w.NOT_COORDINATOR,
            "El participante no redirigió")
    if concurrent:
        with ThreadPoolExecutor(max_workers=5) as workers:
            entries = [entry for group in workers.map(send, range(5)) for entry in group]
    else:
        entries = send(0)
    require(len({e[2] for e in entries}) == len(entries), "Secuencias repetidas")
    bank.nodes[participant].kill()
    bank.nodes[participant].start()
    token, data, seq, client = entries[-1]
    require(bank.final(token, data, client=client)[1] == seq, "Cambió la secuencia del reintento")
    require(bank.submit(token, data + b"!", client=client)[0] == w.CONFLICT, "Conflicto de ID no detectado")
    result = bank.compare(len(entries))
    sample = (bank.path / f"{bank.coordinator}.inspect").read_text()
    for _, data, seq, _ in entries:
        require(any(line.startswith(f"sequence={seq} ") and line.endswith(f"command={data.hex()}")
                    for line in sample.splitlines()), "Bytes binarios no preservados")
    return result


def faults(bank, label):
    bank.start()
    coordinator = bank.coordinator
    participant = next(id for id in IDS if id != coordinator)
    token, command = uuid.uuid4().bytes, b"\x00\xff\x80demo"
    if label == "A":
        bank.nodes[participant].kill()
        require(bank.submit(token, command)[0] == w.PENDING, "Se confirmó sin participante")
        bank.decision()
        bank.nodes[participant].start()
        bank.final(token, command, w.ABORTED)
        return bank.compare(0, 1)
    if label == "B":
        tx = w.Tx(2, uuid.uuid4().bytes, coordinator, "bench", token, command)
        vote = bank.send(participant, w.PREPARE, uuid.uuid4().bytes, coordinator, tx.encode())
        require(vote[2] == 0, "Un participante aceptó un salto en la secuencia")
        # El siguiente PREPARE válido sigue siendo admisible.
        bank.final(token, command)
        return bank.compare(1)
    if label in ("I", "J"):
        bank.final(token, command)
        kind = w.PREPARE if label == "I" else w.COMMIT
        with bank.lock:
            body = next(body for a, b, stage, msg, body in bank.trace
                        if a == coordinator and b == participant and stage == "request" and msg == kind)
        response = bank.client.exchange((HOST, bank.nodes[participant].port), body)
        _, _, correlation, _, reader = w.header(body)
        tx = w.read_payload(kind, reader)
        require(w.correlate(response, participant, correlation, kind, (tx.sequence, tx.xid))[2] == 1,
                "Reintento de protocolo rechazado")
        incompatible = w.ABORT if kind == w.COMMIT else w.COMMIT
        if kind == w.COMMIT:
            require(bank.send(participant, incompatible, uuid.uuid4().bytes, coordinator, tx.encode())[2] == 0,
                    "El participante aceptó una decisión contradictoria")
        return bank.compare(1)
    if label == "O":
        body = w.encode(w.SUBMIT, token, "bench", w.command_bytes(command))
        with socket.create_connection((HOST, bank.nodes[coordinator].port), timeout=2) as conn:
            conn.sendall(dm2.frame(body))
            # Confirmar que se generó la respuesta pero simular que el cliente no la procesa.
            conn.recv(4, socket.MSG_PEEK)
        bank.final(token, command)
        return bank.compare(1)
    if label == "K":
        link = bank.links[coordinator, participant]
        link.set_blocked(True)
        bank.submit(token, command)
        bank.decision()
        require(link.stats()["drops"] > 0, "No se cortó TCP durante PREPARE")
        link.set_blocked(False)
        bank.final(token, command, w.ABORTED)
        return bank.compare(0, 1)

    if label in {"C", "D", "L", "M", "N"}:
        gate = bank.pause("response", w.PREPARE | 0x80, src=coordinator, dst=participant)
        aborted = True
    elif label in {"E", "F"}:
        bank.mute_queries()
        gate = bank.pause("request", w.COMMIT, src=coordinator, occurrence=2 if label == "F" else 1)
        aborted = False
    else:  # G, H
        gate = bank.pause("response", w.COMMIT | 0x80, src=coordinator, dst=participant)
        aborted = False
    arrived, release, points = gate
    try:
        bank.submit(token, command)
        require(arrived.wait(6), f"No alcanzó punto DM2 {label}")
        if label in {"C", "N", "G"}:
            victim = bank.nodes[points[0][1]]
            victim.kill()
            inspection = victim.inspect()
            expected = "PREPARED" if label in ("C", "N") else "COMMIT"
            require(any(f"status={expected}" in line for line in inspection), str(inspection))
            (bank.path / "before-recovery.inspect").write_text("\n".join(inspection), encoding="utf-8")
            release.set()
            if aborted:
                bank.decision()
            victim.start()
        elif label in {"D", "E", "F", "M"}:
            old = bank.nodes[coordinator]
            old.kill()
            inspection = old.inspect()
            expected = "STARTED" if aborted else "COMMIT"
            require(any(f"status={expected}" in line for line in inspection), str(inspection))
            (bank.path / "before-recovery.inspect").write_text("\n".join(inspection), encoding="utf-8")
            release.set()
            if label == "D":
                # Mantener caído al coordinador durante más de un presupuesto RPC.
                time.sleep(.85)
                waiting = bank.nodes[participant]
                waiting.kill()
                snapshot = waiting.inspect()
                require(any("status=PREPARED" in line for line in snapshot),
                        "El participante resolvió PREPARED sin conocer la decisión")
                (bank.path / "blocked-participant.inspect").write_text("\n".join(snapshot), encoding="utf-8")
                waiting.start()
            old.start()
        elif label == "L":
            link = bank.links[coordinator, participant]
            link.set_blocked(True)
            bank.links[participant, coordinator].set_blocked(True)
            release.set()
            bank.decision()
            require(all(node.alive for node in bank.nodes.values()), "Se detuvo un proceso durante la partición")
            link.set_blocked(False)
            bank.links[participant, coordinator].set_blocked(False)
        else:
            release.set()
        bank.final(token, command, w.ABORTED if aborted else w.COMMITTED)
    finally:
        release.set()
    result = bank.compare(0, 1) if aborted else bank.compare(1)
    result["fault_link"] = points[0]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--only", choices=("normal", "faults", "concurrent", "demo"))
    args = parser.parse_args()
    path = args.workdir or Path(tempfile.mkdtemp(prefix="dog-2pc-"))
    path.mkdir(parents=True, exist_ok=True)
    if any(path.iterdir()):
        parser.error("--workdir debe estar vacío")
    print(f"Evidencia 2PC en {path}", flush=True)
    report = {"date_utc": datetime.now(timezone.utc).isoformat(), "system": platform.platform(),
              "python": platform.python_version(), "environment": "TCP loopback", "cases": []}
    for tool in ("java", "kotlinc"):
        output = subprocess.run([tool, "-version"], check=True, capture_output=True, text=True)
        report[tool] = (output.stdout + output.stderr).strip()
    if not args.skip_build:
        build_java()
        build_kotlin()
    failures = 0

    def run(label, coordinator, operation):
        nonlocal failures
        started = time.monotonic()
        bank = Bank(path / label, coordinator)
        case = {"name": label, "coordinator": coordinator, "status": "PASS",
                "configuration": {id: {"language": n.lang, "port": n.port, "peers": n.peers}
                                  for id, n in bank.nodes.items()}}
        try:
            case.update(operation(bank))
        except (OSError, RuntimeError, ValueError, StepFailed, subprocess.SubprocessError) as error:
            case.update(status="FAIL", error=str(error))
            failures += 1
        finally:
            bank.close()
        case["seconds"] = time.monotonic() - started
        report["cases"].append(case)
        print(f"[{case['status']}] {label}: {case.get('error', case.get('hashes'))}", flush=True)

    def demo(bank):
        bank.start()
        node = next(id for id in IDS if id != bank.coordinator)
        before, interrupted, after = (uuid.uuid4().bytes for _ in range(3))
        first = bank.final(before, b"demo\x00\xff")
        print(f"Primera transacción confirmada en secuencia {first[1]}", flush=True)
        bank.nodes[node].kill()
        require(bank.submit(interrupted, b"interrumpida")[0] == w.PENDING, "Se confirmó sin todos")
        eventually(lambda: "decision=2 sequence=2" in bank.nodes[bank.coordinator].log_text(),
                   "No se observó ABORT durable")
        print(f"Participante {node} caído; ABORT decidido, ACK pendiente", flush=True)
        bank.nodes[node].start()
        bank.final(interrupted, b"interrumpida", w.ABORTED)
        recovered = bank.final(after, "después".encode("utf-8"))
        print(f"Participante recuperado; secuencia {recovered[1]} confirmada", flush=True)
        return bank.compare(2, 1)

    if args.only == "demo":
        run("demo", "n1", demo)
    if args.only in (None, "normal"):
        for coordinator in IDS:
            run(f"normal-{coordinator}", coordinator, normal)
    if args.only in (None, "faults"):
        for i, label in enumerate("ABCDEFGHIJKLMNO"):
            run(f"fault-{label}", IDS[i % len(IDS)], lambda bank: faults(bank, label))
    if args.only in (None, "concurrent"):
        run("concurrent-100", "n3", lambda bank: normal(bank, 20, True))
    (path / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    with (path / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
        fields = ("name", "coordinator", "status", "seconds", "commit", "abort", "pending", "hashes", "error")
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report["cases"])
    print(f"{'FAIL' if failures else 'PASS'}: {len(report['cases']) - failures}/{len(report['cases'])}")
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
