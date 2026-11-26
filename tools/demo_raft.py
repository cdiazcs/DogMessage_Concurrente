"""Demostración Raft trilingüe con caídas y evidencias locales de loopback."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import time
import uuid

from build_all import build_java, build_kotlin
from cluster_lib import HOST, StepFailed, dm2
from raft_cli import send
from raft_config import load_config
from raft_evidence import export
from raft_monitor import render
from trilingual_cluster_test import Round, canonical, eventually, require, reserve_ports


def run_demo(workdir, report):
    bank = Round(workdir / "nodes", reserve_ports(3), False)
    config = workdir / "cluster.json"
    config.write_text(json.dumps({
        "algorithm": "raft", "members": list(bank.nodes),
        "nodes": [{"id": n.node_id, "language": n.lang, "host": HOST, "port": n.port,
                   "data_dir": str(n.data_dir)} for n in bank.nodes.values()]
    }, indent=2) + "\n", encoding="utf-8")
    addresses = load_config(config)
    result = report["scenarios"][0]
    result.update(bank.details)
    stages = result["stages"]

    def stage(name, **evidence):
        stages.append({"event": name, **evidence})
        print(f"[PASS] {name} {json.dumps(evidence, ensure_ascii=False)}", flush=True)

    def election(exclude=()):
        return eventually(lambda: bank.cluster.find_leader(.1, exclude=exclude), "No se eligió líder")

    def commands(start_node, begin, count):
        last = 0
        for i in range(begin, begin + count):
            message = bank.message("demo", b"comando-demo\x00\xff" + bytes([i]))
            key = (message.header.sender_id, str(uuid.UUID(bytes=message.header.request_id)))
            with (workdir / "client.log").open("a", encoding="utf-8") as log:
                def trace(line):
                    entry = f"{i + 1:02d} request_id={key[1]} {line}"
                    log.write(entry + "\n")
                    print(f"  {entry}", flush=True)

                target, response, elapsed = send(addresses, message, 9, 2, start_node, emit=trace)
                require(response is not None and response.status == dm2.COMMITTED, "Comando no confirmado")
                trace(f"COMMITTED nodo={target} index={response.log_index} elapsed_ms={elapsed * 1000:.1f}")
            last = bank.record((key, response.log_index, message.payload.command.hex()))
        return last

    def positions(node, expected):
        lines = node.inspect()
        (bank.path / f"checkpoint-{node.node_id}.inspect").write_text("\n".join(lines) + "\n", encoding="utf-8")
        entries = canonical(lines)
        recovered = {(e[3], e[4]): (e[0], e[5]) for e in entries if e[2] == "COMMAND"}
        require(recovered == expected and len(entries) == len(recovered) + sum(e[2] == "NO_OP" for e in entries),
                "El seguidor recuperó un prefijo incorrecto")

    try:
        start = time.monotonic()
        for node in bank.nodes.values():
            node.start()
        initial = election()
        term = initial.state()["term"]
        election_time = time.monotonic() - start
        bank.cluster.wait_commit(bank.nodes.values(), 1)
        result["initial"] = bank.details["initial"] = {"node": initial.node_id, "term": term}
        result["election_s"] = {"initial": election_time}
        stage("Elección inicial y NO_OP comprometido", **result["initial"])
        render(addresses, 1, bank.path)

        follower = next(node for node in bank.nodes.values() if node is not initial)
        index = commands(follower.node_id, 0, 10)
        bank.cluster.wait_commit(bank.nodes.values(), index)
        stage("Diez comandos comprometidos", index=index)
        follower.kill()
        stage("Caída del seguidor", node=follower.node_id)
        index = commands(initial.node_id, 10, 5)
        stage("Cinco comandos con mayoría", index=index)
        start = time.monotonic()
        follower.start()
        bank.cluster.wait_commit([follower], index)
        result["recovery_s"] = {"follower": time.monotonic() - start}
        follower.stop()
        positions(follower, bank.expected)
        follower.start()
        stage("Prefijo de quince comandos verificado en el seguidor", node=follower.node_id)

        initial.kill()
        stage("Caída del líder", node=initial.node_id)
        start = time.monotonic()
        successor = election([initial.node_id])
        new_term = successor.state()["term"]
        require(new_term > term, "El nuevo líder no aumentó el término")
        result["successor"] = {"node": successor.node_id, "term": new_term}
        result["election_s"]["failover"] = time.monotonic() - start
        stage("Reelección", **result["successor"])
        index = commands(successor.node_id, 15, 5)
        stage("Cinco comandos con el nuevo líder", index=index)
        start = time.monotonic()
        initial.start()
        bank.cluster.wait_commit([initial], index)
        result["recovery_s"]["former_leader"] = time.monotonic() - start
        stage("Líder anterior recuperado", node=initial.node_id)
        result.update(bank.finish(index))
        require(result["commands"] == 20, "La demo no confirmó veinte comandos únicos")
        stage("Comparación de prefijos y hashes", sha256=result["sha256"], commands=20)
    finally:
        bank.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--export", type=Path, dest="destination")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    root = (args.workdir or Path(tempfile.mkdtemp(prefix="dog-raft-demo-"))).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        parser.error("--workdir debe estar vacío")
    report = {"date_utc": datetime.now(timezone.utc).isoformat(), "environment": "TCP loopback",
              "os": platform.platform(), "python": platform.python_version(),
              "scenarios": [{"case": "demo-raft", "status": "FAIL", "stages": []}]}
    print(f"Registros e inspecciones: {root}", flush=True)
    result = report["scenarios"][0]
    start = time.monotonic()
    try:
        for tool in ("java", "kotlinc"):
            version = subprocess.run([tool, "-version"], check=True, capture_output=True, text=True, timeout=20)
            report[tool] = (version.stdout + version.stderr).strip()
        if not args.skip_build:
            build_java()
            build_kotlin()
        run_demo(root, report)
        result["status"] = "PASS"
    except (OSError, ValueError, StepFailed, subprocess.SubprocessError) as error:
        result["error"] = str(error)
        print(f"[FAIL] {error}", flush=True)
    finally:
        result["duration_s"] = time.monotonic() - start
        (root / "results.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.destination:
        try:
            export(root, args.destination)
        except (OSError, ValueError) as error:
            print(f"[FAIL] Exportación: {error}", flush=True)
            return 1
    print(f"{result['status']}: demostración Raft")
    return int(result["status"] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
