"""Banco real DMB1/DOG sobre Raft y 2PC, con fallos en procesos y archivos."""
from argparse import ArgumentParser
import base64
from concurrent.futures import ThreadPoolExecutor
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
from threading import Event
import zipfile
import zlib

from chat_cluster_test import write_config
from chat_model import ATTACH, Operation, session_hash
from chat_service import Consensus
from cluster_lib import HOST
from gateway_cluster_test import TLSClient, applied, check_interfaces, gateway, make
from gateway_test_certs import create
from media_store import BLOCK, STATUS, remote
from trilingual_cluster_test import Round, eventually, reserve_ports
from two_phase_commit_test import Bank
import two_pc_wire as w

ROOT = Path(__file__).resolve().parents[1]


def fixtures():
    def png_chunk(tag, payload):
        return struct.pack(">I", len(payload)) + tag + payload + struct.pack(">I", zlib.crc32(tag + payload))

    png = b"\x89PNG\r\n\x1a\n"
    png += png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    png += png_chunk(b"IDAT", zlib.compress(b"\x00\x0d\x94\x88")) + png_chunk(b"IEND", b"")

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = []
    for index, obj in enumerate((b"<</Type/Catalog/Pages 2 0 R>>",
                                  b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
                                  b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 100 100]>>"), 1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref = len(pdf)
    pdf.extend(b"xref\n0 4\n0000000000 65535 f \n")
    for offset in offsets:
        pdf.extend(f"{offset:010d} 00000 n \n".encode())
    pdf.extend(f"trailer\n<</Root 1 0 R/Size 4>>\nstartxref\n{xref}\n%%EOF\n".encode())

    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr(zipfile.ZipInfo("sintetico.bin"), bytes(range(256)) * 90)
    return {"pixel.png": png, "pagina.pdf": bytes(pdf), "prueba.zip": raw.getvalue()}


class Replica:
    def __init__(self, root, node, port):
        self.node = node
        self.port = port
        self.root = root / node
        self.log = root / (node + ".log")
        self.process = None
        if node == "n2":
            self.base = [sys.executable, str(ROOT / "nodes/python/media_store.py")]
        else:
            artifact = ROOT / ("build/java" if node == "n1" else "build/kotlin/dog-messenger-kotlin.jar")
            self.base = ["java", "-cp", str(artifact), "dogmessenger.media.BlobServer"]

    def start(self):
        with self.log.open("ab") as stream:
            self.process = subprocess.Popen(self.base + ["--node", self.node, "--port", str(self.port),
                     "--data-dir", str(self.root)], stdout=stream, stderr=subprocess.STDOUT)

        def listening():
            if self.process.poll() is not None:
                raise RuntimeError(f"DMB1 {self.node} terminó: {self.log.read_text()}")
            try:
                with socket.create_connection((HOST, self.port), timeout=.2):
                    return True
            except OSError:
                return False

        eventually(listening, f"No inició almacén {self.node}")

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=6)

    def available(self, sha, size, secret):
        try:
            return self.call(STATUS, sha, size, secret)[:9] == struct.pack(">BQ", 1, size)
        except (OSError, EOFError, ValueError):
            return False

    def call(self, operation, sha, size, secret):
        return remote((HOST, self.port), secret, self.node, operation, sha, size)


class MediaBank(Bank):
    def __init__(self, *args):
        self.action = None
        super().__init__(*args)

    def observe(self, src, dst, stage, raw):
        original = super().observe(src, dst, stage, raw)
        try:
            algorithm, kind, _, _, reader = w.header(raw)
        except ValueError:
            return original
        if self.action is not None and algorithm == w.ALGORITHM and kind in (w.PREPARE, w.COMMIT) and stage == "request":
            tx = w.read_payload(kind, reader)
            if tx.command.startswith(b"CHAT1" + bytes([ATTACH])):
                return self.action(src, dst, kind, tx.request_id) and original
        return original


def run(root, output, algo, tls, files, browser):
    root.mkdir()
    bank = Round(root / "consensus", reserve_ports(3), False) if algo == "raft" else MediaBank(root / "consensus", "n1")
    for node in bank.nodes.values():
        node.chat_feed = True
    replica_root = root / "replicas"
    replica_root.mkdir()
    replicas = {node: Replica(replica_root, node, port) for node, port in zip(bank.nodes, reserve_ports(3))}
    config = root / "cluster.json"
    write_config(config, bank, algo)
    media = root / "media.json"
    media.write_text(json.dumps({"algorithm": algo, "nodes": {n: [HOST, r.port] for n, r in replicas.items()}}))
    data_dir = root / "gateway-data"
    key = secrets.token_hex(32)
    secret = bytes.fromhex(os.environ["DOG_MEDIA_KEY"])
    ports = reserve_ports(2)
    client = TLSClient(ports, tls / "ca.pem")
    evidence, events = [], []
    log = output / (algo + "-gateway.log")

    def req(op, data, token=None, web=False):
        response = client.call(make(op, data), token, web)
        assert response["ok"], response.get("error")
        return response["result"]

    def register():
        users, tokens = [], []
        for i in range(3):
            password = secrets.token_hex(14)
            username = f"files{i}"
            result = applied(client.call(make("register", {"username": username, "display": f"Usuario {i}",
                                                            "password": password})))
            users.append(result["application"]["user"])
            login = make("login", {"username": username, "password": password})
            login["min_cursor"] = result["position"]
            tokens.append(applied(client.call(login))["session"])
        return users, tokens

    def prepare(name, content, conv, token, web=False):
        info = {"filename": name, "size": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                "content_type": "application/octet-stream", "conversation": conv}
        init = make("upload-init", info)
        def attempt():
            initial = client.call(init, token, web)
            if initial.get("error") in {"AUTHORIZATION_UNAVAILABLE", "APPLIED_CURSOR_UNAVAILABLE"}:
                return None
            assert initial.get("result", {}).get("transfer") == init["id"], initial
            return initial
        eventually(attempt, "No se pudo inicializar el upload", timeout=20)
        return init, make("upload-complete", {"transfer": init["id"]})

    def upload(init, content, token, start=0, end=None, web=False):
        for offset in range(start, len(content), BLOCK):
            if end is not None and offset >= end:
                break
            block = content[offset:offset + BLOCK]
            observed = req("upload-chunk", {"transfer": init["id"], "offset": offset,
                           "block": base64.b64encode(block).decode("ascii")}, token, web)
            assert observed["received"] == offset + len(block)

    def wait_outcome(publish, token, state="COMMITTED", web=False):
        wanted = ("COMMITTED", "ABORTED", "REJECTED", "FAILED")
        last = {}
        observation = publish
        if state == "PENDING":
            response = client.call(publish, token, web)
            assert response["ok"], response.get("error")
            observation = make("upload-status", {"transfer": publish["data"]["transfer"]})
        def attempt():
            response = client.call(observation, token, web)
            if response.get("error") in {"AUTHORIZATION_UNAVAILABLE", "APPLIED_CURSOR_UNAVAILABLE"}:
                return None
            assert response["ok"], response.get("error")
            outcome = response["result"]
            last.update({key: outcome.get(key) for key in ("status", "reason", "replicas", "outcome")})
            if outcome["status"] in wanted or outcome["status"] == state:
                assert outcome["status"] == state, outcome
                return outcome
        try:
            return eventually(attempt, "Publicación sin resultado esperado", timeout=30)
        except Exception:
            print(f"Último estado de publicación: {last}", flush=True)
            raise

    def download(id_, token, web=False):
        def ready():
            result = req("download-init", {"attachment": id_}, token, web)
            return result if result["status"] == "READY" else None
        info = eventually(ready, "Descarga no disponible", timeout=20)
        output = bytearray()
        for offset in range(0, info["size"], BLOCK):
            result = req("download-chunk", {"attachment": id_, "offset": offset}, token, web)
            assert result["offset"] == offset
            output.extend(base64.b64decode(result["block"], validate=True))
        assert hashlib.sha256(output).hexdigest() == info["sha256"]
        return bytes(output)

    def history(conversation, token):
        return req("history", {"conversation": conversation, "after": "0", "limit": 100}, token)["messages"]

    def note(name, content, outcome, received, replicas_):
        sha = hashlib.sha256(content).hexdigest()
        found = hashlib.sha256(received).hexdigest()
        assert sha == found
        evidence.append({"filename": name, "size": len(content), "original": sha, "download": found,
                         "replicas": replicas_, "attachment": outcome["outcome"]["application"]["message"]})

    try:
        for node in bank.nodes.values():
            node.start()
        for replica in replicas.values():
            replica.start()
        if algo == "raft":
            eventually(lambda: bank.cluster.find_leader(.1), "Sin líder")
        with gateway(log, config, tls, ports, key, media=(media, data_dir)):
            try:
                replicas["n2"].call(STATUS, "ab" * 32, 32, bytes(32))
            except (OSError, EOFError):
                pass
            else:
                raise AssertionError("DMB1 aceptó clave errónea")
            users, tokens = register()
            conv = applied(client.call(make("direct", {"peer": users[1]}), tokens[0]))["application"]["conversation"]
            group = applied(client.call(make("group-create", {"name": "Equipo", "members": [users[1]]}),
                                        tokens[0]))["application"]["conversation"]
            name = "prueba.zip"
            content = files[name]
            initial, complete = prepare(name, content, conv, tokens[0], web=True)
            upload(initial, content, tokens[0], end=BLOCK, web=True)
            assert client.call(make("upload-status", {"transfer": initial["id"]}), tokens[2])["error"] == "UNKNOWN_TRANSFER"
            assert client.call(complete, tokens[0])["error"] == "INCOMPLETE_BLOB"
        # El proceso del gateway termina con un bloque persistido.
        with gateway(log, config, tls, ports, key, media=(media, data_dir)):
            assert req("upload-status", {"transfer": initial["id"]}, tokens[0])["received"] == BLOCK
            upload(initial, content, tokens[0], start=BLOCK, web=True)
            victim = replicas["n3"]
            victim.stop()
            if algo == "raft":
                leader = bank.cluster.find_leader()
                follower = next(node for node in bank.nodes.values() if node is not leader)
                follower.kill()
                outcome = wait_outcome(complete, tokens[0], web=True)
                assert len(outcome["replicas"]) == 2
                follower.start()
                leader.kill()
                leader_data = replicas[leader.node_id]
                leader_data.stop()
                eventually(lambda: bank.cluster.find_leader(.1, exclude=[leader.node_id]), "Sin reelección")
                assert download(complete["id"], tokens[1]) == content
                leader.start()
                if leader_data is not victim:
                    leader_data.start()
            else:
                withheld = wait_outcome(complete, tokens[0], "PENDING")
                assert withheld["reason"] == "INSUFFICIENT_DURABILITY" and len(withheld["replicas"]) == 2
                assert all(row["id"] != complete["id"] for row in history(conv, tokens[0]))
                victim.start()
                arrived, release = Event(), Event()
                def cut(src, dst, kind, request_id):
                    if kind == w.COMMIT and request_id.hex() == complete["id"] and not arrived.is_set():
                        arrived.set()
                        release.wait(8)
                        return False
                    return True
                bank.action = cut
                try:
                    response = client.call(complete, tokens[0])
                    assert response["ok"]
                    assert arrived.wait(10), "Faltó COMMIT durable multimedia"
                    bank.nodes["n1"].kill()
                finally:
                    release.set()
                    bank.action = None
                bank.nodes["n1"].start()
                outcome = wait_outcome(complete, tokens[0])
                assert len(outcome["replicas"]) == 3
            if victim.process.poll() is not None:
                victim.start()
            assert wait_outcome(complete, tokens[0])["outcome"] == outcome["outcome"]
            rows = history(conv, tokens[0])
            assert sum(row["id"] == complete["id"] for row in rows) == 1
            assert client.call(make("download-init", {"attachment": complete["id"]}), tokens[2],
                               web=True)["error"] == "FORBIDDEN"
            downloaded = download(complete["id"], tokens[1], web=True)
            note(name, content, outcome, downloaded, outcome["replicas"])
            events.append("corte de gateway durante carga, reanudación, permisos y publicación idempotente")
            events.append("mayoría y reelección" if algo == "raft" else "sin tres copias, pendiente. Caída tras COMMIT durable")

            if algo == "2pc":
                initial_abort, abort = prepare("abortada.bin", b"ensayo de aborto", conv, tokens[0])
                upload(initial_abort, b"ensayo de aborto", tokens[0])
                target = bytes.fromhex(abort["id"])
                arrived, release = Event(), Event()
                preparing = set()
                def before_decision(src, dst, kind, request_id):
                    if kind == w.PREPARE and request_id == target:
                        preparing.add(dst)
                        if len(preparing) == 2:
                            arrived.set()
                            release.wait(8)
                            return False
                    return True
                bank.action = before_decision
                try:
                    assert client.call(abort, tokens[0])["ok"]
                    assert arrived.wait(10), "Faltó PREPARED multimedia"
                    weak = Consensus(config, bytes.fromhex(os.environ["DOG_CHAT_KEY"])).state()
                    assert abort["id"] not in weak.messages
                    bank.nodes["n1"].kill()
                finally:
                    release.set()
                    bank.action = None
                bank.nodes["n1"].start()
                wait_outcome(abort, tokens[0], "ABORTED")
                assert all(row["id"] != abort["id"] for row in history(conv, tokens[0]))
                wrong = make("send", {})
                fake = Operation(ATTACH, bytes.fromhex(wrong["id"]),
                                 (session_hash(tokens[0]), conv, wrong["id"], "FILE", "falso.bin",
                                  "application/octet-stream", "1", "f" * 64,
                                  ",".join(n + ":" + "0" * 64 for n in replicas)))
                assert bank.submit(fake.request, fake.encode(bytes.fromhex(os.environ["DOG_CHAT_KEY"])),
                                   client="chat-app")[0] == w.CONFLICT
                events.append("PREPARED invisible, caída antes de decidir, ABORT y rechazo de recibos falsos")
            else:
                leader = eventually(lambda: bank.cluster.find_leader(.1), "Sin líder para prueba de quórum")
                peers = [node for node in bank.nodes.values() if node is not leader]
                stopped_upload, no_quorum = prepare("sin-quorum.bin", b"quorum", conv, tokens[0])
                upload(stopped_upload, b"quorum", tokens[0])
                for peer in peers:
                    peer.kill()
                denied = client.call(no_quorum, tokens[0])
                assert denied.get("error") == "AUTHORIZATION_UNAVAILABLE", denied
                weak = Consensus(config, bytes.fromhex(os.environ["DOG_CHAT_KEY"])).state()
                assert no_quorum["id"] not in weak.messages
                for peer in peers:
                    peer.start()
                wait_outcome(no_quorum, tokens[0])
                events.append("sin mayoría, adjunto invisible hasta restablecer el quórum")

            def concurrent(name, token, web):
                data = files[name]
                init, publish = prepare(name, data, group, token, web)
                upload(init, data, token, web=web)
                outcome = wait_outcome(publish, token, web=web)
                return name, publish["id"], outcome

            def text():
                command = make("send", {"conversation": group, "text": "Texto concurrente 🐶"})
                def attempt():
                    reply = client.call(command, tokens[0])
                    result = reply.get("result", {})
                    return applied(reply) if result.get("application", {}).get("status") == "APPLIED" else None
                return eventually(attempt, "Texto concurrente no confirmado", timeout=20)

            with ThreadPoolExecutor(max_workers=3) as executor:
                uploads = [executor.submit(concurrent, name, tokens[i], i != 0)
                           for i, name in enumerate(("pixel.png", "pagina.pdf"))]
                sent = executor.submit(text)
                observed = [work.result() for work in uploads]
                assert sent.result()["consensus"] == "COMMITTED"
            for name, ident, outcome in observed:
                note(name, files[name], outcome, download(ident, tokens[1]), outcome["replicas"])
            image = observed[0][1]
            assert len([row for row in history(group, tokens[0]) if row["type"] == "IMAGE"]) == 1
            applied(client.call(make("group-remove", {"conversation": group, "member": users[1]}), tokens[0]))
            assert client.call(make("download-chunk", {"attachment": image, "offset": 0}), tokens[1])["error"] == "FORBIDDEN"
            events.append("dos cargas concurrentes y texto confirmado, PNG, PDF y ZIP verificados")
            events.append("descarga revocada tras expulsión del grupo")

        # Retirar el caché y dañar una copia local para obligar la recuperación por TCP.
        item = evidence[0]
        sha, size = item["original"], item["size"]
        shutil.rmtree(data_dir / "downloads")
        damaged = replicas["n1"].root / (sha + ".bin")
        damaged.write_bytes(b"\x00" * size)
        for node in bank.nodes.values():
            node.kill()
        for replica in replicas.values():
            replica.stop()
        for node in bank.nodes.values():
            node.start()
        for replica in replicas.values():
            replica.start()
        if algo == "raft":
            eventually(lambda: bank.cluster.find_leader(.1), "Sin líder al reiniciar")
        with gateway(log, config, tls, ports, key, media=(media, data_dir)):
            assert download(complete["id"], tokens[0]) == content
            for replica in replicas.values():
                eventually(lambda: replica.available(sha, size, secret), "Copia sin reparar")
            assert damaged.read_bytes() == content
            checkpoint = client.call(make("conversations"), tokens[0])
            assert checkpoint["ok"], checkpoint
            cursor = int(checkpoint["cursor"])
            hashes = {node: Consensus(config, bytes.fromhex(os.environ["DOG_CHAT_KEY"]), reader=node)
                      .state(cursor).public_digest() for node in bank.nodes}
            assert len(set(hashes.values())) == 1
            if browser:
                check_interfaces(root, log, config, tls, ports, key, output, browser, media=(media, data_dir))
                events.append("PNG de Chrome descargado en Swing y PDF de Swing descargado en Chrome y verificado por HTTPS")
        events.append("reinicio completo, réplica corrupta reparada desde otra copia y convergencia")
        return {"algorithm": algo, "status": "PASS", "files": evidence, "checks": events, "hashes": hashes}
    finally:
        for replica in replicas.values():
            replica.stop()
            if replica.log.exists():
                shutil.copyfile(replica.log, output / f"{algo}-dmb-{replica.node}.log")
        bank.close()


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--export", required=True, type=Path)
    parser.add_argument("--browser", help="Ejecutable Chrome para pruebas de interfaces")
    parser.add_argument("--only", choices=("raft", "2pc"))
    args = parser.parse_args()
    args.export.mkdir(parents=True, exist_ok=False)
    os.environ["DOG_CHAT_KEY"] = secrets.token_hex(32)
    os.environ["DOG_MEDIA_KEY"] = secrets.token_hex(32)
    os.environ["DOG_MEDIA_RECEIPT_KEY"] = secrets.token_hex(32)
    files = fixtures()
    for name, data in files.items():
        (args.export / name).write_bytes(data)
    results = []
    with tempfile.TemporaryDirectory(prefix="dog-media-") as directory:
        root = Path(directory)
        create(root / "tls", [HOST])
        try:
            for algo in ([args.only] if args.only else ["raft", "2pc"]):
                result = run(root / algo, args.export, algo, root / "tls", files, args.browser)
                results.append(result)
                print(f"PASS multimedia {algo}", flush=True)
        finally:
            (args.export / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            with (args.export / "summary.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, ["algorithm", "filename", "size", "original", "download",
                                                  "replicas", "attachment"])
                writer.writeheader()
                for case in results:
                    for item in case["files"]:
                        writer.writerow({"algorithm": case["algorithm"], **item})


if __name__ == "__main__":
    main()
