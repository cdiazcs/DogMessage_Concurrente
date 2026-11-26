"""Operación Raft con procesos TCP reales y pruebas de artefactos locales."""
from __future__ import annotations

import contextlib
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from cluster_lib import Cluster, HOST
from raft_compare import compare_files
from raft_config import load_config
from raft_evidence import export
from trilingual_cluster_test import reserve_ports


class RaftToolsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="dog herramientas con espacios ")
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)

    def config(self, ports=(19111, 19112, 19113)):
        return {"algorithm": "raft", "members": ["n1", "n2", "n3"],
                "nodes": [{"id": f"n{i}", "language": "python", "host": HOST, "port": port,
                           "data_dir": f"data/node {i}"} for i, port in enumerate(ports, 1)]}

    def write(self, name, document):
        path = self.path / name
        path.write_text(json.dumps(document), encoding="utf-8")
        return path

    def test_config_validates_members_and_endpoints(self):
        config = self.config()
        path = self.write("network config.json", config)
        self.assertEqual(load_config(path)["n3"].data_dir, "data/node 3")
        for key, invalid in (("port", True), ("host", "0.0.0.0"), ("id", "n1"), ("language", "ruby")):
            with self.subTest(field=key):
                changed = self.config()
                changed["nodes"][1][key] = invalid
                with self.assertRaises(ValueError):
                    load_config(self.write("invalid.json", changed))
        config["members"][1] = "n1"
        with self.assertRaises(ValueError):
            load_config(self.write("members.json", config))

    def test_compare_distinguishes_lag_and_first_divergence(self):
        def inspection(name, term, commit, pending=False):
            path = self.path / name
            path.write_text(f"role=FOLLOWER term=9 commitIndex={commit}\n"
                            f"index=1 term={term} type=NO_OP status=COMMITTED client=null requestId=null command=-\n"
                            + ("index=2 term=2 type=COMMAND status=" + ("PENDING" if pending else "COMMITTED")
                               + " client=c1 requestId=00000000-0000-0000-0000-000000000001 command=00ff\n"),
                            encoding="utf-8")
            return path
        a = inspection("node a.inspect", 1, 2)
        b = inspection("node b.inspect", 1, 1, True)
        corrupt = inspection("node c.inspect", 2, 1, True)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(compare_files([a, b]), 0)
            self.assertEqual(compare_files([a, corrupt]), 1)
        self.assertIn("PREFIJO COMPARTIDO", output.getvalue())
        self.assertIn("divergencia en índice 1", output.getvalue())

    def test_export_keeps_inspections_and_skips_persistent_data(self):
        source = self.path / "working results"
        source.mkdir()
        (source / "data-n1").mkdir()
        (source / "data-n1" / "raft-state.bin").write_bytes(b"untouched")
        (source / "n1.inspect").write_text("state", encoding="utf-8")
        (source / "n1.log").write_text("runtime", encoding="utf-8")
        (source / "results.json").write_text(json.dumps({"scenarios": [
            {"case": "demo", "commands": 20, "status": "PASS", "initial": {"node": "n1", "term": 7}}]}),
            encoding="utf-8")
        destination = self.path / "exported evidence"
        with contextlib.redirect_stdout(io.StringIO()):
            export(source, destination)
        self.assertFalse((destination / "data-n1").exists())
        self.assertEqual((source / "data-n1" / "raft-state.bin").read_bytes(), b"untouched")
        self.assertEqual((destination / "n1.inspect").read_text(encoding="utf-8"), "state")
        with (destination / "summary.csv").open(encoding="utf-8", newline="") as stream:
            row = next(csv.DictReader(stream))
        self.assertEqual((row["commands"], row["initial_node"], row["initial_term"]), ("20", "n1", "7"))
        with self.assertRaises(FileExistsError):
            export(source, destination)

    def tool(self, name, config, *args):
        return subprocess.run([sys.executable, str(ROOT / "tools" / name), "--config", str(config), *args],
                              cwd=self.path, capture_output=True, text=True, encoding="utf-8", timeout=12,
                              env=dict(os.environ, PYTHONIOENCODING="utf-8"))

    def test_console_and_monitor_against_real_servers(self):
        ports = reserve_ports(3)
        config = self.write("config with spaces.json", self.config(ports))
        cluster = Cluster([(f"n{i}", "python", port) for i, port in enumerate(ports, 1)], self.path)
        self.addCleanup(cluster.stop_all)
        for node in cluster.nodes.values():
            node.start()
        leader = cluster.find_leader()
        self.assertIsNotNone(leader)
        others = [n for n in cluster.nodes.values() if n is not leader]
        cluster.wait_commit(cluster.nodes.values(), 1)

        token = uuid.uuid4().hex
        common = ["--client", "test-cli", "--request-id", token]
        first = self.tool("raft_cli.py", config, *common, "--hex", "00ff80", "--start-node", others[0].node_id)
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        self.assertIn("NOT_LEADER", first.stderr)
        self.assertIn("COMMITTED nodo=", first.stdout)
        duplicate = self.tool("raft_cli.py", config, *common, "--hex", "00ff80")
        self.assertEqual(duplicate.returncode, 0, duplicate.stdout + duplicate.stderr)
        self.assertEqual(first.stdout.split("index=")[1].split()[0], duplicate.stdout.split("index=")[1].split()[0])
        conflict = self.tool("raft_cli.py", config, *common, "--text", "diferente")
        self.assertEqual(conflict.returncode, 2, conflict.stdout + conflict.stderr)
        self.assertIn("REQUEST_ID_CONFLICT", conflict.stdout)

        monitor = self.tool("raft_monitor.py", config, "--logs-dir", str(self.path))
        self.assertEqual(monitor.returncode, 0, monitor.stderr)
        self.assertIn("alcanzables=3/3", monitor.stdout)
        self.assertIn(f"{leader.node_id}@", monitor.stdout)
        self.assertIn("quórum_confirmado=DESCONOCIDO", monitor.stdout)

        for node in others:
            node.kill()
        monitor = self.tool("raft_monitor.py", config, "--logs-dir", str(self.path))
        self.assertIn("NO DISPONIBLE role=DESCONOCIDO", monitor.stdout)
        self.assertIn("alcanzables=1/3", monitor.stdout)
        retry_id = uuid.uuid4().hex
        pending = ["--client", "test-cli", "--request-id", retry_id, "--binary-test", "--start-node", leader.node_id]
        attempt = self.tool("raft_cli.py", config, *pending, "--attempts", "4")
        self.assertEqual(attempt.returncode, 3, attempt.stdout + attempt.stderr)
        self.assertGreaterEqual(attempt.stderr.count("RETRY"), 2)
        self.assertIn("ERROR", attempt.stderr)
        self.assertIn(retry_id, attempt.stdout)
        for node in others:
            node.start()
        restored = self.tool("raft_cli.py", config, *pending)
        self.assertEqual(restored.returncode, 0, restored.stdout + restored.stderr)
        self.assertIn("COMMITTED", restored.stdout)
        final_index = int(restored.stdout.split("index=")[1].split()[0])
        cluster.wait_commit(cluster.nodes.values(), final_index)
        processes = [n.proc for n in cluster.nodes.values()]
        cluster.stop_all()
        self.assertTrue(all(p.poll() is not None for p in processes))
        for node in cluster.nodes.values():
            hits = [line for line in node.inspect() if str(uuid.UUID(retry_id)) in line]
            self.assertEqual(len(hits), 1)
            self.assertIn("status=COMMITTED", hits[0])


if __name__ == "__main__":
    unittest.main()
