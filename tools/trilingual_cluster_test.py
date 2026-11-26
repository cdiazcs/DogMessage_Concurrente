"""Matriz Raft Java, Python y Kotlin con procesos reales y enlaces TCP controlados."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
import uuid

from build_all import build_java, build_kotlin
from cluster_lib import HOST, Cluster, ROLE_LINE, StepFailed, committed_entries, dm2

LANGS = ("java", "python", "kotlin")
IDS = ("n1", "n2", "n3")
ENTRY = re.compile(r"index=(\d+) term=(\d+) type=(COMMAND|NO_OP) status=COMMITTED "
                   r"client=(\S+) requestId=(\S+) command=(\S+)")


def require(ok, message):
    if not ok:
        raise StepFailed(message)


def eventually(predicate, message, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.04)
    raise StepFailed(message)


def read_exact(sock, count):
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise EOFError("trama incompleta en el enlace")
        data.extend(chunk)
    return data


def relay(source, target, gate=None):
    prefix = read_exact(source, 4)
    size, = struct.unpack(">I", prefix)
    if not 0 < size <= dm2.MAX_BODY:
        raise ValueError("longitud fuera de DM2")
    body = read_exact(source, size)
    if gate is not None and not gate(bytes(body)):
        return False
    target.sendall(prefix + body)
    return True


class Link:
    """Un enlace dirigido transporta una solicitud y una respuesta por conexión."""
    def __init__(self, port, destination, request_gate=None, response_gate=None):
        self.listener = socket.socket()
        try:
            self.listener.bind((HOST, port))
            self.listener.listen()
            self.listener.settimeout(.1)
        except BaseException:
            self.listener.close()
            raise
        self.port = self.listener.getsockname()[1]
        self.destination = destination
        self.request_gate = request_gate
        self.response_gate = response_gate
        self.lock = threading.Lock()
        self.active = ()
        self.blocked = False
        self.delay = 0.0
        self.dropped = self.delayed = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def set_blocked(self, blocked):
        with self.lock:
            self.blocked = blocked
            if blocked:
                for connection in self.active:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

    def stats(self):
        with self.lock:
            return {"drops": self.dropped, "delayed_responses": self.delayed, "response_delay_s": self.delay}

    def run(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.stop.is_set():
                    raise
                return
            with client, socket.socket() as peer:
                try:
                    with self.lock:
                        if self.blocked:
                            self.dropped += 1
                            continue
                        self.active = (client, peer)
                    client.settimeout(.6)
                    peer.settimeout(.6)
                    peer.connect((HOST, self.destination))
                    if not relay(client, peer, self.request_gate):
                        continue
                    with self.lock:
                        delay = self.delay
                        if delay:
                            self.delayed += 1
                    if delay:
                        self.stop.wait(delay)
                    relay(peer, client, self.response_gate)
                except (OSError, EOFError, ValueError):
                    # Una conexión cortada o un destino caído se observa como error de transporte.
                    pass
                finally:
                    with self.lock:
                        self.active = ()

    def close(self):
        self.stop.set()
        self.set_blocked(True)
        self.listener.close()
        self.thread.join(2)
        require(not self.thread.is_alive(), "No terminó el hilo de un enlace TCP")


def reserve_ports(count):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind((HOST, 0))
        return [sock.getsockname()[1] for sock in sockets]


def canonical(lines):
    entries = []
    for line in committed_entries(lines):
        match = ENTRY.fullmatch(line)
        require(match, f"Inspección inválida: {line}")
        index, term, kind, client, request, command = match.groups()
        require(int(index) == len(entries) + 1, f"Índice no consecutivo: {index}")
        entries.append([int(index), int(term), kind, client, request,
                        bytes.fromhex(command).hex() if kind == "COMMAND" else "-"])
    return entries


def compare_prefix(reference, actual, node):
    for index, (a, b) in enumerate(zip(reference, actual), 1):
        require(a == b, f"{node}: divergencia en índice {index}: {a} != {b}")
    require(len(reference) == len(actual), f"{node}: longitud distinta, primer índice divergente "
            f"{min(len(reference), len(actual)) + 1}")


class Round:
    def __init__(self, path, ports, proxy):
        self.path = path
        path.mkdir(parents=True)
        self.cluster = Cluster(zip(IDS, LANGS, ports), path)
        self.nodes = self.cluster.nodes
        self.links = {}
        self.expected = {}
        self.details = {"ports": dict(zip(IDS, ports)), "logs": [str(n.log_path) for n in self.nodes.values()]}
        if proxy:
            try:
                for src in IDS:
                    for dst in IDS:
                        if src != dst:
                            link = Link(0, self.nodes[dst].port)
                            while link.port in ports:
                                link.close()
                                link = Link(0, self.nodes[dst].port)
                            self.links[src, dst] = link
                            self.nodes[src].peers[dst] = link.port
            except BaseException:
                self.close()
                raise
            self.details["links"] = {f"{a}->{b}": link.port for (a, b), link in self.links.items()}

    def close(self):
        try:
            self.cluster.stop_all()
        finally:
            for link in self.links.values():
                link.close()

    def force(self, id):
        first = self.nodes[id]
        first.start()
        eventually(lambda: first.state()["term"] >= 4, "No inició elecciones estando solo")
        require(first.state()["role"] != "LEADER" and first.state()["commit"] == 0, "Liderazgo sin mayoría")
        for node in self.nodes.values():
            if node is not first:
                node.start()
        eventually(lambda: first.state()["role"] == "LEADER", f"No se eligió al candidato {id}", timeout=3)
        term = first.state()["term"]
        self.cluster.wait_commit(self.nodes.values(), 1)
        require(first.state()["role"] == "LEADER" and first.state()["term"] == term, "Cambió el líder inicial")
        self.details["initial"] = {"node": id, "term": term}
        return first

    def call(self, node, msg):
        return self.cluster.client.call((HOST, node.port), node.node_id, msg).payload

    def message(self, client, data):
        return dm2.Message(dm2.Header(dm2.RAFT, dm2.CLIENT_COMMAND, uuid.uuid4().bytes, client), dm2.ClientCommand(data))

    def confirm(self, node, msg, expected_index=None):
        key = (msg.header.sender_id, str(uuid.UUID(bytes=msg.header.request_id)))
        def attempt():
            response = self.call(node, msg)
            require(isinstance(response, dm2.ClientResult), f"Respuesta inesperada: {response}")
            if response.status == dm2.RETRY:
                return None
            require(response.status == dm2.COMMITTED, f"Se esperaba COMMITTED: {response}")
            return response
        response = eventually(attempt, "El comando no se confirmó")
        if expected_index is not None:
            require(response.log_index == expected_index, "El reintento cambió de índice")
        return key, response.log_index, msg.payload.command.hex()

    def record(self, result):
        key, index, data = result
        require(key not in self.expected, f"Solicitud duplicada: {key}")
        self.expected[key] = (index, data)
        return index

    def send(self, node, count, label):
        saved = []
        for i in range(count):
            msg = self.message(label, b"dog\x00\xff\x80" + i.to_bytes(4, "big"))
            saved.append((msg, self.record(self.confirm(node, msg))))
        return saved

    def replay(self, node, msg, index):
        self.confirm(node, msg, index)
        bad = dm2.Message(msg.header, dm2.ClientCommand(msg.payload.command + b"\x00"))
        require(self.call(node, bad) == dm2.ClientResult(dm2.REQUEST_ID_CONFLICT, 0),
                "Un reintento con bytes distintos no devolvió REQUEST_ID_CONFLICT")

    def without_quorum(self, node, msg):
        before = node.state()["commit"]
        replies = [self.call(node, msg), self.call(node, msg)]
        require(replies == [dm2.ClientResult(dm2.RETRY, 0)] * 2, f"Resultado minoritario: {replies}")
        require(node.state()["commit"] == before, "commitIndex avanzó sin mayoría")
        self.details["minority"] = {"node": node.node_id, "replies": ["RETRY", "RETRY"], "commit": before}

    def finish(self, index):
        self.cluster.wait_commit(self.nodes.values(), index)
        reference = None
        hashes = {}
        commit = {}
        leaders = []
        initial = self.details.get("initial")
        leader = self.cluster.find_leader()
        require(leader is not None, "No hay líder al cerrar la inspección")
        self.cluster.wait_commit(self.nodes.values(), leader.state()["commit"])
        # Mantener los heartbeats hasta detener a los seguidores evita otra elección.
        for node in self.nodes.values():
            if node is not leader:
                node.stop()
        leader.stop()
        for node in self.nodes.values():
            lines = node.inspect()
            (self.path / f"{node.node_id}.inspect").write_text("\n".join(lines) + "\n", encoding="utf-8")
            entries = canonical(lines)
            actual = {}
            for i, _, kind, client, token, data in entries:
                if kind == "COMMAND":
                    require((client, token) not in actual, f"{node.node_id}: comando duplicado en índice {i}")
                    actual[client, token] = (i, data)
            if reference is not None:
                compare_prefix(reference, entries, node.node_id)
            reference = entries
            require(actual == self.expected, f"{node.node_id}: comandos confirmados distintos")
            first = lines[0]
            applied = [entry[0] for entry in entries if entry[2] == "COMMAND"]
            require(f"commitIndex={len(entries)} " in first and f"lastApplied={len(entries)} " in first
                    and f"appliedCommands={applied}" in first, f"Reconstrucción incorrecta en {node.node_id}")
            term = int(re.search(r"\bterm=(\d+)", first).group(1))
            if initial and node.node_id == initial["node"]:
                require(term >= initial["term"], "El líder perdió su término al reiniciar")
            canonical_bytes = (json.dumps(entries, ensure_ascii=True, separators=(",", ":")) + "\n").encode()
            (self.path / f"{node.node_id}.committed.json").write_bytes(canonical_bytes)
            hashes[node.node_id] = hashlib.sha256(canonical_bytes).hexdigest()
            commit[node.node_id] = {"term": term, "index": len(entries)}
            events = [m.groups() for m in ROLE_LINE.finditer(node.log_text())]
            leaders.extend({"node": event[0], "term": int(event[2])}
                           for event in events if event[1] == "LEADER" and event[4] == "majority")
        for elected in leaders:
            require(any(e[1] == elected["term"] and e[2] == "NO_OP" for e in reference)
                    or elected["term"] not in {e[1] for e in reference},
                    f"Falta NO_OP del término {elected['term']}")
        require(reference and reference[0][2] == "NO_OP", "Falta NO_OP inicial")
        self.details.update(sha256=hashes, state=commit, leaders=sorted(leaders, key=lambda e: e["term"]),
                            commands=len(self.expected))
        if self.links:
            self.details["transport"] = {f"{a}->{b}": link.stats() for (a, b), link in self.links.items()}
        return self.details


def run_case(round_, name, id, follower=None):
    nodes = round_.nodes
    peers = [n for n in nodes.values() if n.node_id != id]
    leader = round_.force(id)
    index = 1

    if name == "initial":
        saved = round_.send(leader, 20, "inicial")
        msg, index = saved[-1]
        round_.cluster.wait_commit(nodes.values(), index)
        for node in peers:
            require(round_.call(node, msg) == dm2.ClientResult(dm2.NOT_LEADER, 0, id),
                    f"Respuesta incorrecta del seguidor {node.node_id}")
        round_.replay(leader, msg, index)

    elif name == "follower":
        victim = nodes[follower]
        _, index = round_.send(leader, 5, "antes")[-1]
        round_.cluster.wait_commit(nodes.values(), index)
        victim.kill()
        _, index = round_.send(leader, 5, "durante")[-1]
        start = time.monotonic()
        victim.start()
        round_.cluster.wait_commit([victim], index)
        round_.details["recovery_s"] = time.monotonic() - start

    elif name in {"leader", "dedup"}:
        msg, previous_index = round_.send(leader, 5, "antes")[-1]
        round_.cluster.wait_commit(nodes.values(), previous_index)
        before = leader.state()["term"]
        leader.kill()
        start = time.monotonic()
        if name == "dedup":
            preferred = nodes[follower]
            other = next(node for node in peers if node is not preferred)
            other.kill()
            eventually(lambda: preferred.state()["term"] >= before + 3, "No tomó ventaja el sucesor")
            require(preferred.state()["role"] != "LEADER", "El sucesor se eligió sin mayoría")
            other.start()
        current = eventually(lambda: round_.cluster.find_leader(.1, exclude=[id]), "No hubo reelección")
        if name == "dedup":
            require(current is preferred, "Se adelantó el sucesor alternativo")
        term = current.state()["term"]
        require(term > before, "La reelección no aumentó el término")
        round_.cluster.wait_commit(peers, previous_index + 1)
        round_.replay(current, msg, previous_index)
        round_.details["dedup"] = f"{leader.lang}->{current.lang}"
        round_.details["successor"] = {"node": current.node_id, "term": term}
        _, index = round_.send(current, 5, "despues")[-1]
        leader.start()
        round_.cluster.wait_commit([leader], index)
        require(leader.state()["role"] == "FOLLOWER", "El antiguo líder no volvió como seguidor")
        round_.details["recovery_s"] = time.monotonic() - start

    elif name == "quorum":
        _, index = round_.send(leader, 5, "antes")[-1]
        round_.cluster.wait_commit(nodes.values(), index)
        for node in peers:
            node.kill()
        msg = round_.message("minor", b"reintento\x00\xff")
        round_.without_quorum(leader, msg)
        start = time.monotonic()
        peers[0].start()
        index = round_.record(round_.confirm(leader, msg))
        round_.replay(leader, msg, index)
        peers[1].start()
        round_.cluster.wait_commit(nodes.values(), index)
        round_.details["recovery_s"] = time.monotonic() - start

    elif name == "partition":
        _, index = round_.send(leader, 5, "antes")[-1]
        round_.cluster.wait_commit(nodes.values(), index)
        # Primero comprobar un corte dirigido con retardo en el enlace alternativo.
        outgoing = round_.links[id, peers[0].node_id]
        delayed = round_.links[id, peers[1].node_id]
        outgoing.set_blocked(True)
        with delayed.lock:
            delayed.delay = .065
        _, index = round_.send(leader, 1, "asimetria")[-1]
        eventually(lambda: outgoing.stats()["drops"] and delayed.stats()["delayed_responses"],
                   "No se observó corte dirigido y respuesta demorada")
        with delayed.lock:
            delayed.delay = 0
        outgoing.set_blocked(False)
        round_.cluster.wait_commit(nodes.values(), index)
        term = leader.state()["term"]
        for (src, dst), link in round_.links.items():
            if id in (src, dst):
                link.set_blocked(True)
        losing = round_.message("aislado", b"sufijo\x00\xff")
        round_.without_quorum(leader, losing)
        require(all(n.alive for n in nodes.values()), "La partición terminó algún proceso")
        current = eventually(lambda: round_.cluster.find_leader(.1, exclude=[id]), "La mayoría no eligió")
        require(current.state()["term"] > term, "La mayoría conservó un término obsoleto")
        _, index = round_.send(current, 5, "mayoria")[-1]
        start = time.monotonic()
        for link in round_.links.values():
            link.set_blocked(False)
        round_.cluster.wait_commit(nodes.values(), index)
        require(leader.state()["term"] > term and leader.state()["role"] == "FOLLOWER",
                "El aislado no reconoció al líder de término superior")
        round_.details["successor"] = {"node": current.node_id, "term": current.state()["term"]}
        round_.details["recovery_s"] = time.monotonic() - start

    elif name == "concurrent":
        follower = peers[0]
        def worker(i):
            output = []
            for j in range(20):
                msg = round_.message(f"client{i}", bytes((i, 0, 255, j)))
                if j == 0:
                    require(round_.call(follower, msg) == dm2.ClientResult(dm2.NOT_LEADER, 0, id),
                            "El seguidor perdió la referencia del líder")
                output.append(round_.confirm(leader, msg))
            return output
        with ThreadPoolExecutor(max_workers=5) as executor:
            for results in executor.map(worker, range(5)):
                for item in results:
                    index = max(index, round_.record(item))
        require(len({i for i, _ in round_.expected.values()}) == 100, "Los comandos comparten índices")

    return round_.finish(index)


def verify_vote(path, ports, id):
    round_ = Round(path, ports, False)
    node = round_.nodes[id]
    first, second = [other for other in IDS if other != id]
    endpoint = (HOST, node.port)
    def vote(peer):
        return round_.cluster.client.call_rpc(endpoint, id, peer, dm2.RequestVote(1000, 0, 0)).payload
    try:
        node.start()
        require(vote(first) == dm2.VoteResponse(1000, True), "No persistió el primer voto")
        locked = subprocess.run(node.base + node.args() + ["--inspect"], capture_output=True, text=True, timeout=10)
        output = locked.stdout + locked.stderr
        require(locked.returncode != 0 and re.search(r"en uso|bloquear", output),
                f"No se comprobó el bloqueo exclusivo: {output}")
        node.kill()
        lines = node.inspect()
        (path / f"{id}.inspect").write_text("\n".join(lines) + "\n", encoding="utf-8")
        require("term=1000 " in lines[0], "Término no recuperado")
        node.start()
        require(vote(second) == dm2.VoteResponse(1000, False), "Voto distinto admitido tras reiniciar")
        require(vote(first) == dm2.VoteResponse(1000, True), "No se recuperó el voto original")
        return {**round_.details, "term": 1000, "vote": first, "exclusive": True, "lock_message": output.strip()}
    finally:
        round_.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, help="Directorio nuevo para conservar los resultados")
    parser.add_argument("--ports", type=int, nargs=3, metavar=("N1", "N2", "N3"))
    parser.add_argument("--skip-build", action="store_true", help="Reutiliza los compilados existentes")
    args = parser.parse_args()
    if args.ports and (len(set(args.ports)) != 3 or any(not 1 <= p <= 65535 for p in args.ports)):
        parser.error("--ports requiere tres puertos válidos y distintos")
    root = args.workdir or Path(tempfile.mkdtemp(prefix="dog-trilingual-"))
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        parser.error("--workdir debe estar vacío")
    print(f"Evidencias: {root}", flush=True)
    report = {"date_utc": datetime.now(timezone.utc).isoformat(), "os": platform.platform(),
              "python": platform.python_version(), "languages": dict(zip(IDS, LANGS)), "scenarios": []}
    errors = 0
    try:
        for tool in ("java", "kotlinc"):
            executable = shutil.which(tool)
            require(executable, f"Falta {tool}")
            version = subprocess.run([executable, "-version"], check=True, capture_output=True, text=True, timeout=20)
            report[tool] = (version.stdout + version.stderr).strip()
        if not args.skip_build:
            build_java()
            build_kotlin()
        for id in IDS:
            cases = [("initial", None), *[("follower", other) for other in IDS if other != id],
                     ("leader", None), ("quorum", None), ("partition", None), ("vote", None)]
            cases.append(("dedup", None))
            if id == "n3":
                cases.append(("concurrent", None))
            natural_successor = None
            for case, follower in cases:
                if case == "dedup":
                    # Completar el otro sentido saliente del líder inicial.
                    follower = next(other for other in IDS if other not in (id, natural_successor))
                label = f"{id}-{case}" + (f"-{follower}" if follower else "")
                start = time.monotonic()
                result = {"case": label}
                try:
                    ports = args.ports or reserve_ports(3)
                    if case == "vote":
                        result.update(verify_vote(root / label, ports, id))
                    else:
                        # La ventaja se obtiene con elecciones sin quórum. Repetir solo el
                        # arranque si otro candidato se adelanta antes de entrar al escenario.
                        for attempt in range(1, 4):
                            bank = Round(root / label / f"attempt-{attempt}", ports, case == "partition")
                            try:
                                result.update(bank.details)
                                result.update(run_case(bank, case, id, follower))
                                if case == "leader":
                                    natural_successor = result["successor"]["node"]
                                break
                            except StepFailed as error:
                                if bank.details.get("initial") or str(error) not in {
                                    f"No se eligió al candidato {id}", "Cambió el líder inicial"} or attempt == 3:
                                    raise
                                result.setdefault("startup_retries", []).append(str(error))
                                print(f"[RETRY] {label}: {error}", flush=True)
                            finally:
                                result.update(bank.details)
                                bank.close()
                    result["status"] = "PASS"
                except (StepFailed, OSError, subprocess.SubprocessError) as error:
                    result.update(status="FAIL", error=str(error))
                    errors += 1
                result["duration_s"] = time.monotonic() - start
                report["scenarios"].append(result)
                print(f"[{result['status']}] {label} {result.get('error', result.get('commands', 'voto recuperado'))}",
                      flush=True)
    finally:
        (root / "results.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"{'FAIL' if errors else 'PASS'}: {len(report['scenarios']) - errors}/{len(report['scenarios'])}")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
