"""Banco de chat de tres procesos reales para Raft y 2PC en TCP loopback."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import secrets
import tempfile
import time

from build_all import build_java, build_kotlin
from chat_model import ADD, DIRECT, GROUP, REMOVE, REVOKE, SEND, Rejected, identity, session_hash
from chat_service import Chat, Consensus
from cluster_lib import HOST
from raft_evidence import export
from trilingual_cluster_test import Round, eventually, require, reserve_ports
from two_phase_commit_test import Bank
import two_pc_wire as twopc


def write_config(path, bank, algorithm):
    config = {"algorithm": algorithm, "members": list(bank.nodes),
              "nodes": [{"id": n.node_id, "language": n.lang, "host": HOST, "port": n.port}
                        for n in bank.nodes.values()]}
    if algorithm == "2pc":
        config["coordinator"] = bank.coordinator
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="ascii")


def applied(chat, operation):
    result = chat.submit(operation)
    require(result.get("application", {}).get("status") == "APPLIED",
            f"Operación {operation.kind} no aplicada: {result.get('consensus')}")
    return result


def run(root, algorithm, secret):
    bank = Round(root / "nodes", reserve_ports(3), False) if algorithm == "raft" else Bank(root / "nodes", "n1")
    started = time.monotonic()
    events, protected = [], []
    for node in bank.nodes.values():
        node.chat_feed = True
    config = root / "cluster.json"
    write_config(config, bank, algorithm)
    chat = Chat(Consensus(config, secret, attempts=35))

    def checkpoint(cursor):
        projections = {id: Consensus(config, secret, reader=id).state(cursor) for id in bank.nodes}
        # Todos los nodos deben haber aplicado el mismo prefijo de negocio.
        hashes = {id: projection.public_digest() for id, projection in projections.items()}
        require(len(set(hashes.values())) == 1, "Proyecciones de chat distintas")
        leader_view = next(iter(projections.values()))
        require(all(state.sessions == leader_view.sessions and state.users == leader_view.users
                    for state in projections.values()), "Sesiones o credenciales no convergieron")
        return projections, hashes

    def event(label, **values):
        events.append({"event": label, **values})
        print(f"[{algorithm}] {label} {values}", flush=True)

    try:
        for node in bank.nodes.values():
            node.start()
        if algorithm == "raft":
            eventually(lambda: bank.cluster.find_leader(.1), "Sin líder Raft")
        try:
            Consensus(config, bytes(value ^ 0xFF for value in secret), attempts=1).state()
            raise AssertionError("Feed accesible sin la clave")
        except Rejected as error:
            require(str(error) == "APPLIED_CURSOR_UNAVAILABLE", "Respuesta inesperada del feed")
        users, tokens = [], []
        for i in range(5):
            username = f"user{i}"
            password = secrets.token_hex(12)
            protected.append(password)
            registration = chat.register_operation(username, f"Usuario {i}", password)
            protected.extend(registration.fields[-3::2])
            registered = applied(chat, registration)
            users.append(registered["application"]["user"])
            token, operation = chat.login_operation(username, password, minimum=registered["position"])
            protected.extend((token, operation.fields[1]))
            tokens.append(token)
            applied(chat, operation)
        event("registro y login", users=len(users))

        def operation(kind, actor, *args):
            return chat.operation(kind, session_hash(tokens[actor]), *args)

        private = applied(chat, operation(DIRECT, 0, users[1]))["application"]["conversation"]
        for sender in (0, 1):
            applied(chat, operation(SEND, sender, private, identity(), f"mensaje privado {sender}"))
        group = identity()
        applied(chat, operation(GROUP, 0, group, "Grupo de prueba", ",".join(users)))
        initial = operation(SEND, 1, group, identity(), "texto grupal 🐶")
        first = applied(chat, initial)
        require(chat.submit(initial) == first, "Reintento no recuperó el índice")
        conflict = chat.operation(SEND, *initial.fields[:-1], "bytes distintos", token=initial.request)
        require(chat.submit(conflict)["consensus"] == "CONFLICT", "No detectó conflicto de identidad")
        try:
            chat.read(tokens[2], "history", cid=private)
            raise AssertionError("Se reveló una conversación privada")
        except Rejected as error:
            require(str(error) == "FORBIDDEN", "Se esperaba rechazo de permisos")
        applied(chat, operation(REMOVE, 0, group, users[4]))
        denied = chat.submit(operation(SEND, 4, group, identity(), "no autorizado"))
        require(denied["consensus"] == "COMMITTED" and denied["application"]["status"] == "REJECTED",
                "Un rechazo de negocio se confundió con consenso")
        try:
            chat.read(tokens[4], "history", cid=group)
            raise AssertionError("Un expulsado leyó el grupo")
        except Rejected as error:
            require(str(error) == "FORBIDDEN", "Se esperaba expulsión")
        applied(chat, operation(ADD, 0, group, users[4]))
        event("conversaciones, permisos y deduplicación", private=private, group=group)

        if algorithm == "raft":
            leader = eventually(lambda: bank.cluster.find_leader(.1), "Sin líder Raft")
            follower = next(node for node in bank.nodes.values() if node is not leader)
            follower.kill()
            sent = applied(chat, operation(SEND, 0, group, identity(), "mayoría Raft"))
            follower.start()
            checkpoint(sent["position"])
            leader.kill()
            successor = eventually(lambda: bank.cluster.find_leader(.1, exclude=[leader.node_id]), "Sin reelección")
            require(chat.submit(initial) == first, "Se perdió deduplicación tras reelección")
            leader.start()
            # Sin mayoría se admite incertidumbre, nunca COMMITTED.
            for node in bank.nodes.values():
                if node is not successor:
                    node.kill()
            isolated = Chat(Consensus(config, secret, attempts=3))
            op = operation(SEND, 0, group, identity(), "reintento Raft")
            uncertain = isolated.submit(op)
            require(uncertain["consensus"] == "INDETERMINATE", "Confirmación sin mayoría")
            require(not any(m["id"] == op.fields[2] for m in chat.read(tokens[0], "history", cid=group)["messages"]),
                    "Un comando sin aplicar apareció en el historial Raft")
            for node in bank.nodes.values():
                if not node.alive:
                    node.start()
            applied(chat, op)
            event("caída y recuperación Raft", previous=leader.node_id, successor=successor.node_id)
        else:
            victim = bank.nodes["n2"]
            victim.kill()
            interrupted = operation(SEND, 0, group, identity(), "transacción abortada")
            cut = Chat(Consensus(config, secret, attempts=5)).submit(interrupted)
            require(cut["consensus"] in ("PENDING", "INDETERMINATE"), "Se confirmó 2PC sin todos")
            require(not any(m["id"] == interrupted.fields[2] for m in chat.read(tokens[0], "history", cid=group)["messages"]),
                    "PREPARED o ABORT apareció en historial")
            victim.start()
            require(chat.submit(interrupted)["consensus"] == "ABORTED", "Faltó recuperar ABORT")

            # Interceptar el primer COMMIT después de la decisión durable, antes de entregarlo.
            arrived, release, _ = bank.pause("request", twopc.COMMIT, src=bank.coordinator)
            disrupted = operation(SEND, 0, group, identity(), "commit tras caída del coordinador")
            raw = disrupted.encode(secret)
            try:
                bank.submit(disrupted.request, raw, client="chat-app")
                require(arrived.wait(6), "No se alcanzó COMMIT durable")
                bank.nodes[bank.coordinator].kill()
            finally:
                release.set()
            bank.nodes[bank.coordinator].start()
            applied(chat, disrupted)
            require(chat.submit(initial) == first, "Se perdió deduplicación tras recuperar coordinador")
            event("caída y recuperación 2PC", pending=cut["consensus"], final="ABORTED", coordinator_replayed=True)

        def worker(sender):
            return [applied(chat, operation(SEND, sender, group, identity(), f"concurrente {sender}-{i}"))
                    for i in range(20)]

        concurrent_started = time.monotonic()
        with ThreadPoolExecutor(max_workers=5) as workers:
            rows = [row for batch in workers.map(worker, range(5)) for row in batch]
        concurrent_seconds = time.monotonic() - concurrent_started
        highest = max(row["position"] for row in rows)
        require(len({row["application"]["message"] for row in rows}) == 100, "Duplicación concurrente")
        before = chat.read(tokens[0], "sync", cid=group, after=0, limit=3)
        require(before["more"] and len(before["messages"]) == 3, "Paginación inválida")
        after = chat.read(tokens[0], "sync", cid=group, after=before["next_cursor"], limit=100,
                          minimum=highest, strong=True)
        positions = [message["position"] for message in before["messages"] + after["messages"]]
        require(positions == sorted(set(positions)), "Cursor no ordenado o mensaje repetido")
        revocation = applied(chat, operation(REVOKE, 4))
        try:
            chat.read(tokens[4], "groups", minimum=revocation["position"])
            raise AssertionError("Sesión revocada aceptada")
        except Rejected as error:
            require(str(error) == "AUTH_REQUIRED", "Falló revocación")
        final = revocation["position"]
        previous, hashes = checkpoint(final)
        for node in bank.nodes.values():
            node.kill()
        for node in bank.nodes.values():
            node.start()
        projections, recovered = checkpoint(final)
        require(hashes == recovered and all(projections[id].users == previous[id].users and
                                            projections[id].sessions == previous[id].sessions for id in previous),
                "Estado diferente después del reinicio")
        for node in bank.nodes.values():
            output = node.log_text()
            require(not any(secret in output for secret in protected), "Se filtró un secreto en el log")
        event("concurrencia y reconstrucción", sent=100, same_hash=True, observed_seconds=round(concurrent_seconds, 3))
        return {"status": "PASS", "case": algorithm, "commands": 100,
                "sha256": hashes, "observations": events, "duration_s": time.monotonic() - started,
                "messages_per_node": {node: len(state.messages) for node, state in projections.items()}}
    finally:
        bank.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--export", type=Path, dest="destination")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    root = (args.workdir or Path(tempfile.mkdtemp(prefix="dog-chat-"))).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        parser.error("--workdir debe estar vacío")
    if not args.skip_build:
        build_java()
        build_kotlin()
    secret = secrets.token_bytes(32)
    os.environ["DOG_CHAT_KEY"] = secret.hex()
    report = {"date_utc": datetime.now(timezone.utc).isoformat(), "os": platform.platform(),
              "environment": "TCP loopback", "python": platform.python_version(), "scenarios": []}
    print(f"Evidencias: {root}", flush=True)
    for algorithm in ("raft", "2pc"):
        path = root / algorithm
        path.mkdir()
        try:
            result = run(path, algorithm, secret)
        except Exception as error:
            result = {"case": algorithm, "status": "FAIL", "error": str(error)}
            print(f"[FAIL] {algorithm}: {error}", flush=True)
        report["scenarios"].append(result)
    (root / "results.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.destination:
        export(root, args.destination)
    passed = sum(result["status"] == "PASS" for result in report["scenarios"])
    print(f"{'PASS' if passed == 2 else 'FAIL'}: {passed}/2 bancos de chat")
    return int(passed != 2)


if __name__ == "__main__":
    raise SystemExit(main())
