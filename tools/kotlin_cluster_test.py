"""Banco Raft 2D y verificación binaria trilingüe en TCP loopback."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from pathlib import Path
import socket
import tempfile
import time

from build_all import build_java, build_kotlin
from cluster_lib import HOST, Cluster, Report, StepFailed, committed_entries, dm2, dm_node


def require(condition, message):
    if not condition:
        raise StepFailed(message)


def make_cluster(languages, path):
    path.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in languages]
        for listener in sockets:
            listener.bind((HOST, 0))
        specs = [(f"n{i}", lang, listener.getsockname()[1])
                 for i, (lang, listener) in enumerate(zip(languages, sockets), 1)]
    return Cluster(specs, path)


def leader(cluster):
    current = cluster.find_leader()
    require(current is not None, "No se eligió líder")
    return current


def snapshot(node, label):
    lines = node.inspect()
    (node.log_path.parent / f"{label}-{node.node_id}.inspect").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return committed_entries(lines)


def sent_commands(entries):
    return [line.rsplit(" command=", maxsplit=1)[1] for line in entries if " type=COMMAND " in line]


def verify_logs(cluster, label, expected):
    cluster.stop_all()
    logs = [snapshot(n, label) for n in cluster.nodes.values()]
    require(logs[0] == logs[1] == logs[2], f"Prefijos distintos en {label}")
    require(sent_commands(logs[0]) == expected, f"Bytes u orden incorrectos en {label}")
    return f"{len(expected)} comandos y {len(logs[0]) - len(expected)} NO_OP con índices, términos, IDs y bytes idénticos"


def send(cluster, expected, count):
    index = 0
    for _ in range(count):
        data = b"dog\x00\xff" + len(expected).to_bytes(4, "big")
        index, _ = cluster.send_command(data, client_id="bench")
        expected.append(data.hex())
    return index


def request(payload):
    return dm2.Message(dm2.Header(dm2.RAFT, dm2.CLIENT_COMMAND, dm_node.new_request_id(), "bench"),
                       dm2.ClientCommand(payload))


def call(cluster, node, message):
    return cluster.client.call((HOST, node.port), node.node_id, message).payload


def run_kotlin(path, report):
    cluster = make_cluster(("kotlin", "kotlin", "kotlin"), path)
    nodes = list(cluster.nodes.values())
    expected = []
    last = 0
    current = lagging = previous = pending = None

    def start_all():
        for node in nodes:
            node.start()

    def elect():
        nonlocal current
        current = leader(cluster)
        return f"{current.node_id}, término {current.state()['term']}"

    def add(count):
        nonlocal last
        last = send(cluster, expected, count)
        return f"último índice confirmado {last}"

    def lose_follower():
        nonlocal lagging
        current_leader = leader(cluster)
        lagging = next(n for n in nodes if n is not current_leader)
        lagging.kill()
        return lagging.node_id

    def verify_live_prefix(label, count):
        cluster.wait_commit(nodes, last)
        require(len(expected) == count, "Número inesperado de comandos")
        detail = verify_logs(cluster, label, expected)
        start_all()
        return detail

    def recover(node):
        node.start()
        cluster.wait_commit([node], last)
        return f"{node.node_id} alcanzó {last}"

    def lose_leader():
        nonlocal previous
        previous = leader(cluster)
        previous.kill()
        return previous.node_id

    def reelect():
        result = elect()
        require(current is not previous and current.state()["term"] > previous.state()["term"],
                "No se observó un nuevo líder con término superior")
        return result

    def lose_majority():
        nonlocal current
        current = leader(cluster)
        for node in nodes:
            if node is not current:
                node.kill()
        return f"solo quedó {current.node_id}"

    def verify_no_commit():
        nonlocal pending
        before = current.state()["commit"]
        pending = request(b"pending\x00\xff")
        first = call(cluster, current, pending)
        second = call(cluster, current, pending)
        require(first == second == dm2.ClientResult(dm2.RETRY, 0), f"Resultado sin mayoría: {first}, {second}")
        require(current.state()["commit"] == before, "commitIndex avanzó sin mayoría")
        return f"dos RETRY para el mismo ID; commitIndex={before}"

    def restore():
        for node in nodes:
            if node is not current:
                node.start()
        elected = leader(cluster)
        # Los votos comparan el log: el líder anterior conserva el prefijo más reciente.
        outcome = call(cluster, elected, pending)
        require(outcome.status == dm2.COMMITTED, f"Reintento tras restaurar mayoría: {outcome}")
        expected.append(pending.payload.command.hex())
        require(call(cluster, elected, pending) == outcome, "El reintento cambió el índice comprometido")
        cluster.wait_commit(nodes, outcome.log_index)
        return verify_logs(cluster, "41-restauracion", expected)

    try:
        start_all()
        report.step("1. Elección inicial", elect)
        report.step("2. Compromiso y propagación del NO_OP", lambda: cluster.wait_commit(nodes, 1))
        report.step("3. Enviar 20 comandos binarios", lambda: add(20))
        report.step("4. Verificar los 20 comandos", lambda: verify_live_prefix("20-inicial", 20))
        report.step("5. Caída de un seguidor", lose_follower)
        report.step("6. Confirmar diez comandos con mayoría", lambda: add(10))
        report.step("7. Recuperar seguidor atrasado", lambda: recover(lagging))
        report.step("8. Comparar los 30 comandos", lambda: verify_live_prefix("30-seguidor", 30))
        report.step("9. Caída del líder", lose_leader)
        report.step("10. Elección de otro líder", reelect)
        report.step("11. Confirmar otros diez comandos", lambda: add(10))
        report.step("12. Recuperar al antiguo líder", lambda: recover(previous))
        report.step("13. Comparar los 40 comandos", lambda: verify_live_prefix("40-lider", 40))
        report.step("14. Pérdida de mayoría", lose_majority)
        report.step("15. Comprobar ausencia de confirmaciones", verify_no_commit)
        report.step("16. Restaurar el quórum y comparar logs", restore)
    finally:
        cluster.stop_all()


def run_interop(path, report):
    languages = ("java", "python", "kotlin")
    for pos, language in enumerate(languages, 1):
        def round_trip():
            for attempt in range(1, 4):
                cluster = make_cluster(languages, path / language / f"attempt-{attempt}")
                first = cluster.nodes[f"n{pos}"]
                ready = False
                try:
                    first.start()
                    deadline = time.monotonic() + 8
                    while first.state()["term"] < 3:
                        require(time.monotonic() < deadline, f"{language} no inició elecciones")
                        time.sleep(.03)
                    for node in cluster.nodes.values():
                        if node is not first:
                            node.start()
                    current = leader(cluster)
                    if current is first:
                        ready = True
                        break
                    print(f"[RETRY] líder inicial {current.lang}; se esperaba {language} (intento {attempt})",
                          flush=True)
                finally:
                    if not ready:
                        cluster.stop_all()
            require(ready, f"No se obtuvo líder {language} en tres arranques")
            try:
                expected = []
                messages = [request(bytes(range(256)) + bytes([i])) for i in range(6)]
                for msg in messages:
                    outcome = call(cluster, current, msg)
                    require(outcome.status == dm2.COMMITTED, f"Comando no confirmado: {outcome}")
                    expected.append(msg.payload.command.hex())
                require(call(cluster, current, messages[-1]) == outcome, "El reintento no mantuvo el índice")
                cluster.wait_commit(cluster.nodes.values(), outcome.log_index)
                result = verify_logs(cluster, "dm2", expected)
                require("6 comandos y 1 NO_OP" in result, result)
                return result
            finally:
                cluster.stop_all()
        report.step(f"DM2 con líder {language}", round_trip)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interop", action="store_true", help="Tres rondas binarias con líderes de los tres lenguajes")
    parser.add_argument("--workdir", type=Path)
    args = parser.parse_args()
    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="dog-kotlin-cluster-"))
    print(f"Registros e inspecciones en {workdir}", flush=True)
    build_kotlin()
    if args.interop:
        build_java()
    report = Report()
    try:
        if args.interop:
            run_interop(workdir, report)
        else:
            run_kotlin(workdir, report)
    except Exception:
        print(f"FAIL: {report.passed} pasos aprobados. Evidencia en {workdir}", flush=True)
        raise
    print(f"PASS: {report.passed}/{3 if args.interop else 16}. Evidencia en {workdir}")


if __name__ == "__main__":
    main()
