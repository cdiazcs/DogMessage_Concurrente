"""Interoperabilidad E1 con JCA y Web Crypto, sobre servidores reales Raft/2PC."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import shutil
import time
import subprocess
import tempfile

from chat_cluster_test import write_config
from chat_service import Consensus
from cluster_lib import HOST
from gateway_cluster_test import TLSClient, applied, gateway, make
from gateway_test_certs import create
from media_cluster_test import Replica, fixtures
from media_store import BLOCK
from trilingual_cluster_test import Round, eventually, reserve_ports
from test_qr_codec import png_descriptor
from two_phase_commit_test import Bank
import two_pc_wire as twopc

ROOT = Path(__file__).resolve().parents[1]
TRANSIENT = {"APPLIED_CURSOR_UNAVAILABLE", "AUTHORIZATION_UNAVAILABLE", "BACKEND_UNAVAILABLE"}


class Cryptography:
    def __init__(self, stack, args):
        self.proc = stack.enter_context(subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                                        text=True, encoding="utf-8"))
        stack.callback(self.close)

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=6)

    def call(self, op, name, bad=False, **args):
        self.proc.stdin.write(json.dumps({"op": op, "name": name, **args}) + "\n")
        self.proc.stdin.flush()
        output = json.loads(self.proc.stdout.readline())
        if bad:
            assert "error" in output
            return
        assert "result" in output, output
        return output["result"]


def active(reply):
    if reply.get("error") in TRANSIENT:
        return None
    assert reply["ok"], reply.get("error")
    value = reply["result"]
    if value["consensus"] in {"PENDING", "INDETERMINATE"} or value.get("application", {}).get("status") == "NOT_OBSERVED":
        return None
    return value


def run(root, destination, algorithm, tls, java, web):
    root.mkdir()
    bank = Round(root / "nodes", reserve_ports(3), False) if algorithm == "raft" else Bank(root / "nodes", "n1")
    for node in bank.nodes.values():
        node.chat_feed = True
    config = root / "cluster.json"
    write_config(config, bank, algorithm)
    stores = root / "stores"
    stores.mkdir()
    replicas = {node: Replica(stores, node, port) for node, port in zip(bank.nodes, reserve_ports(3))}
    media_config = root / "media.json"
    media_config.write_text(json.dumps({"algorithm": algorithm,
        "nodes": {name: [HOST, replica.port] for name, replica in replicas.items()}}))
    media = (media_config, root / "gateway-data")
    key = secrets.token_hex(32)
    ports = reserve_ports(2)
    client = TLSClient(ports, tls / "ca.pem")
    log = destination / f"{algorithm}-gateway.log"
    checks, texts = [], []
    users, device, sessions, passwords = {}, {}, {}, {}
    evidence = []
    engines = {"alice": java, "bob": web, "carol": web}

    def mutate(req, token=None, tls_web=False):
        return eventually(lambda: active(client.call(req, token, tls_web)), "Operación sin resolución", timeout=20)

    def read(op, data, user):
        req = make(op, data)
        def attempt():
            reply = client.call(req, sessions[user], user == "bob")
            if reply.get("error") in TRANSIENT:
                return None
            assert reply["ok"], reply.get("error")
            return reply
        return eventually(attempt, "No se obtuvo el prefijo autorizado", timeout=15)

    def enroll(name, passphrase, rejected=False):
        register = make("device-register", {"device": device[name]["id"], "exchange": device[name]["exchange"],
                                              "signing": device[name]["signing"]})
        register["data"]["proof"] = engines[name].call("proof", name, request=register["id"], token=passphrase)
        if rejected:
            return register
        result = mutate(register, passphrase, name == "bob")
        assert result["application"]["status"] == "APPLIED", result

    def make_message(who, conversation, directory):
        text = "privado-" + secrets.token_hex(12) + " 🐕"
        texts.append(text.encode())
        request = make("e2ee-send", {"conversation": conversation, "device": device[who]["id"],
                                      "epoch": directory["epoch"]})
        meta = {"conversation": conversation, "message": request["id"], "device": request["data"]["device"],
                "epoch": request["data"]["epoch"]}
        request["data"]["envelope"] = engines[who].call("seal", who, text=text, meta=meta, devices=directory["devices"])
        return text, request, meta

    def round_trip(who, other, conversation):
        directory = read("e2ee-directory", {"conversation": conversation}, who)["result"]
        text, request, meta = make_message(who, conversation, directory)
        confirmation = mutate(request, sessions[who], who == "bob")
        assert confirmation["application"]["status"] == "APPLIED", confirmation
        assert mutate(request, sessions[who], who == "bob") == confirmation
        rows = read("history", {"conversation": conversation, "after": "0", "limit": 100}, other)["result"]["messages"]
        matching = [row for row in rows if row["id"] == request["id"]]
        assert len(matching) == 1 and "text" not in matching[0]
        box = matching[0]["envelope"]
        assert engines[other].call("open", other, box=box, sender=matching[0]["sender_keys"], meta=meta) == text
        return text, box, meta

    def encoded(data):
        return base64.b64encode(data).decode("ascii")

    def encrypted_file(who, other, conversation, filename, content):
        directory = read("e2ee-directory", {"conversation": conversation}, who)["result"]
        complete = make("upload-complete", {})
        message = complete["id"]
        source = engines[who].call("file-seal", who, conversation=conversation, message=message, bytes=encoded(content))
        ciphertext = base64.b64decode(source["bytes"])
        assert ciphertext != content and hashlib.sha256(ciphertext).hexdigest() == source["sha256"]
        meta = {"conversation": conversation, "message": message, "device": device[who]["id"],
                "epoch": directory["epoch"], "purpose": "file"}
        ticket = json.dumps({"filename": filename, "size": len(content), "type": "application/octet-stream",
                             "key": source["key"], "sha256": source["sha256"]})
        box = engines[who].call("seal", who, text=ticket, meta=meta, devices=directory["devices"])
        init = make("upload-encrypted-init", {"conversation": conversation, "message": message,
                    "device": device[who]["id"], "epoch": directory["epoch"], "envelope": box,
                    "sha256": source["sha256"], "size": len(ciphertext)})
        initial = client.call(init, sessions[who], who == "bob")
        assert initial["ok"], initial.get("error")
        complete["data"]["transfer"] = init["id"]
        for offset in range(0, len(ciphertext), BLOCK):
            chunk = make("upload-chunk", {"transfer": init["id"], "offset": offset,
                         "block": encoded(ciphertext[offset:offset + BLOCK])})
            reply = client.call(chunk, sessions[who], who == "bob")
            assert reply["ok"], reply.get("error")
            if offset == 0:
                assert client.call(chunk, sessions[who], who == "bob") == reply
        def completion():
            reply = client.call(complete, sessions[who], who == "bob")
            if reply.get("error") in TRANSIENT:
                return None
            assert reply["ok"], reply.get("error")
            status = reply["result"]
            assert status["status"] not in {"REJECTED", "FAILED", "ABORTED"}, status
            return status if status["status"] == "COMMITTED" else None
        final = eventually(completion, "Adjunto sin compromiso", timeout=30)
        assert len(final["replicas"]) >= (2 if algorithm == "raft" else 3)
        rows = read("history", {"conversation": conversation, "after": "0", "limit": 100}, other)["result"]["messages"]
        row, = [item for item in rows if item["id"] == message]
        assert row["type"] == "E2EE_FILE" and row["attachment"]["filename"] == "adjunto.dgf"
        received_ticket = engines[other].call("open", other, sender=row["sender_keys"],
                                              box=row["envelope"], meta=meta)
        assert json.loads(received_ticket) == json.loads(ticket)
        def ready():
            receipt = read("download-init", {"attachment": message}, other)["result"]
            return receipt if receipt["status"] == "READY" else None
        eventually(ready, "Adjunto cifrado inaccesible", timeout=20)
        data = b"".join(base64.b64decode(read("download-chunk", {"attachment": message, "offset": offset},
                        other)["result"]["block"]) for offset in range(0, len(ciphertext), BLOCK))
        assert data == ciphertext
        params = {"conversation": conversation, "message": message, "key": source["key"], "size": len(content)}
        recovered = engines[other].call("file-open", other, bytes=encoded(data), **params)
        assert base64.b64decode(recovered) == content
        corrupted = bytearray(data)
        corrupted[-1] ^= 1
        engines[other].call("file-open", other, bad=True, bytes=encoded(corrupted), **params)
        engines[other].call("file-open", other, bad=True, bytes=encoded(data[:-1]), **params)
        engines[other].call("file-open", other, bad=True, bytes=encoded(data), **{**params, "message": "f" * 32})
        if len(data) > 20 + 2 * (BLOCK + 16):
            end = 20 + BLOCK + 16
            reverse = data[:20] + data[end:end + BLOCK + 16] + data[20:end] + data[end + BLOCK + 16:]
            engines[other].call("file-open", other, bad=True, bytes=encoded(reverse), **params)
        for replica in replicas.values():
            stored = replica.root / (source["sha256"] + ".bin")
            assert stored.read_bytes() == ciphertext
        evidence.append({"file": filename, "original": hashlib.sha256(content).hexdigest(),
                         "ciphertext": source["sha256"], "replicas": final["replicas"]})
        texts.extend((content, filename.encode(), source["key"].encode()))
        return message

    def prepare_link(name, cursor):
        engines[name] = web
        login = make("login", {"username": algorithm + "_alice", "password": passwords["alice"]})
        login["min_cursor"] = str(cursor)
        sessions[name] = mutate(login)["session"]
        public = device[name] = web.call("generate", name)
        issued = str(int(time.time()))
        challenge = secrets.token_hex(32)
        request = make("link-start", {"device": public["id"], "exchange": public["exchange"],
                       "signing": public["signing"], "challenge": challenge, "issued": issued})
        request["data"]["proof"] = web.call("link-proof", name, purpose="start", request=request["id"],
            token=sessions[name], fields=[challenge, issued])
        result = mutate(request, sessions[name])
        assert result["application"]["status"] == "APPLIED", result
        qr = read("link-qr", {"link": request["id"]}, name)["result"]
        image = base64.b64decode(qr["png"])
        descriptor = png_descriptor(image)
        if name == "linked":
            (destination / f"{algorithm}-qr-publico.png").write_bytes(image)
        assert descriptor.decode("ascii") == qr["descriptor"]
        assert sessions[name].encode() not in descriptor
        value = json.loads(descriptor)
        assert value["device"] == public["id"] and value["id"] == request["id"] and value["challenge"] == challenge
        return value

    def approve(link, signer="alice", timestamp=None):
        stamp = str(int(time.time()) if timestamp is None else timestamp)
        command = make("link-approve", {"link": link["id"], "device": device["alice"]["id"], "observed": stamp})
        command["data"]["proof"] = engines[signer].call("link-proof", signer, purpose="approve",
            request=command["id"], token=sessions["alice"], fields=[link["id"], link["device"], link["challenge"], stamp])
        return command

    def share_and_verify(conversation, original_text):
        original = read("history", {"conversation": conversation, "after": "0", "limit": 100}, "alice")["result"]["messages"]
        candidate = read("history", {"conversation": conversation, "after": "0", "limit": 100}, "linked")["result"]["messages"]
        assert [row["id"] for row in original] == [row["id"] for row in candidate]
        for row in original:
            meta = {"conversation": conversation, "message": row["id"], "device": row["device"],
                    "epoch": row["epoch"], "purpose": "file" if row["type"] == "E2EE_FILE" else "text"}
            web.call("open", "linked", bad=True, box=row["envelope"], sender=row["sender_keys"], meta=meta)
            key = java.call("export-key", "alice", box=row["envelope"], sender=row["sender_keys"], meta=meta)
            texts.append(key.encode())
            grant_meta = {**meta, "device": device["alice"]["id"], "purpose": "grant"}
            envelope = java.call("seal", "alice", text=key, meta=grant_meta, devices=[device["linked"]])
            grant = make("history-grant", {"message": row["id"], "device": device["alice"]["id"],
                         "target": device["linked"]["id"], "envelope": envelope})
            outcome = mutate(grant, sessions["alice"])
            assert outcome["application"]["status"] == "APPLIED"
            assert mutate(grant, sessions["alice"]) == outcome
        history = {"conversation": conversation, "after": "0", "limit": 100}
        cloned = read("history", history, "linked")["result"]["messages"]
        assert [row["id"] for row in cloned] == [row["id"] for row in read("sync", history, "linked")["result"]["messages"]]
        opened = []
        for row in cloned:
            grant = row["history_key"]
            meta = {"conversation": conversation, "message": row["id"], "device": row["device"],
                    "epoch": row["epoch"], "purpose": "file" if row["type"] == "E2EE_FILE" else "text"}
            grant_meta = {**meta, "device": grant["issuer"], "purpose": "grant"}
            key = web.call("open", "linked", box=grant["envelope"], sender=grant, meta=grant_meta)
            result = web.call("open", "linked", box=row["envelope"], sender=row["sender_keys"], meta=meta, historyKey=key)
            opened.append(result)
        assert opened[0] == original_text
        assert {json.loads(text)["filename"] for text in opened[2:]} == {"pixel.png", "pagina.pdf", "prueba.zip"}

    try:
        for node in bank.nodes.values():
            node.start()
        for replica in replicas.values():
            replica.start()
        if algorithm == "raft":
            eventually(lambda: bank.cluster.find_leader(.1), "Sin líder")
        with gateway(log, config, tls, ports, key, media=media):
            for name in engines:
                password = passwords[name] = secrets.token_hex(16)
                username = f"{algorithm}_{name}"
                first = mutate(make("register", {"username": username, "display": name, "password": password}))
                users[name] = first["application"]["user"]
                login = make("login", {"username": username, "password": password})
                login["min_cursor"] = first["position"]
                sessions[name] = mutate(login)["session"]
                device[name] = engines[name].call("generate", name)
                enroll(name, sessions[name])
            same_user = make("login", {"username": algorithm + "_alice", "password": "invalid-test-value"})
            assert client.call(same_user)["error"] == "INVALID_CREDENTIALS"
            forged = enroll("alice", sessions["alice"], rejected=True)
            assert client.call(forged, sessions["carol"])["error"] == "INVALID_DEVICE_PROOF"
            checks.append("registro de identidades y prueba de posesión ligada a la sesión")
            direct = mutate(make("direct", {"peer": users["bob"]}), sessions["alice"])["application"]["conversation"]
            assert mutate(make("e2ee-enable", {"conversation": direct}), sessions["alice"])["application"]["status"] == "APPLIED"
            first_text, first_box, first_meta = round_trip("alice", "bob", direct)
            round_trip("bob", "alice", direct)
            assert "text" not in str(read("history", {"conversation": direct, "after": "0", "limit": 10}, "bob")["result"]["messages"][0].keys())
            for field in ("message", "conversation", "epoch"):
                bad_meta = {**first_meta, field: "f" * len(first_meta[field])}
                web.call("open", "bob", bad=True, box=first_box, sender=device["alice"], meta=bad_meta)
            for column in (1, 2, 4):
                fields = first_box.split(".")
                raw = bytearray(base64.b64decode(fields[column]))
                raw[-1] ^= 1
                fields[column] = base64.b64encode(raw).decode()
                web.call("open", "bob", bad=True, box=".".join(fields), sender=device["alice"], meta=first_meta)
            web.call("open", "bob", bad=True, box=first_box, sender=device["carol"], meta=first_meta)
            web.call("open", "carol", bad=True, box=first_box, sender=device["alice"], meta=first_meta)
            rejected = mutate(make("send", {"conversation": direct, "text": "rechazo-sintetico-en-claro"}), sessions["alice"])
            assert rejected["application"]["reason"] == "E2EE_REQUIRED"
            checks.append("JCA↔Web Crypto sobre TLS/HTTPS, rechazo de firma, ciphertext, nonce, AAD y clave ajena")
            group = mutate(make("group-create", {"name": "Grupo cifrado", "members": [users["bob"], users["carol"]]}),
                           sessions["alice"])["application"]["conversation"]
            assert mutate(make("e2ee-enable", {"conversation": group}), sessions["alice"])["application"]["status"] == "APPLIED"
            round_trip("alice", "carol", group)
            previous = read("e2ee-directory", {"conversation": group}, "alice")["result"]
            _, stale, _ = make_message("alice", group, previous)
            mutate(make("group-remove", {"conversation": group, "member": users["carol"]}), sessions["alice"])
            blocked = mutate(stale, sessions["alice"])
            assert blocked["application"]["reason"] == "E2EE_EPOCH_CHANGED"
            _, next_box, next_meta = round_trip("alice", "bob", group)
            assert next_meta["epoch"] != previous["epoch"]
            web.call("open", "carol", bad=True, box=next_box, sender=device["alice"], meta=next_meta)
            assert client.call(make("history", {"conversation": group, "after": "0", "limit": 10}),
                               sessions["carol"])["error"] == "FORBIDDEN"
            revoked = mutate(make("device-revoke", {"device": device["carol"]["id"]}), sessions["carol"])
            assert revoked["application"]["status"] == "APPLIED"
            assert client.call(make("devices"), sessions["carol"])["error"] == "AUTH_REQUIRED"
            checks.append("grupo, cambio de época, rechazo de un envío antiguo y revocación de sesiones del dispositivo")

            artifacts = fixtures()
            stored = []
            for index, (filename, content) in enumerate(artifacts.items()):
                who, other = ("alice", "bob") if index % 2 == 0 else ("bob", "alice")
                stored.append((encrypted_file(who, other, direct, filename, content), other))
            checks.append("PNG/PDF/ZIP DGF1 cruzados, repetición de bloques y rechazo de alteración, truncado y reordenamiento")
            if algorithm == "raft":
                leader = bank.cluster.find_leader()
                follower = next(n for n in bank.nodes.values() if n is not leader)
                follower.kill()
                round_trip("bob", "alice", group)
                follower.start()
                leader.kill()
                eventually(lambda: bank.cluster.find_leader(.1, exclude=[leader.node_id]), "Sin nuevo líder")
                round_trip("alice", "bob", group)
                leader.start()
                current = eventually(lambda: bank.cluster.find_leader(.1), "Sin líder para probar la minoría")
                directory = read("e2ee-directory", {"conversation": group}, "alice")["result"]
                _, waiting, _ = make_message("alice", group, directory)
                peers = [node for node in bank.nodes.values() if node is not current]
                for peer in peers:
                    peer.kill()
                reply = client.call(waiting, sessions["alice"])
                assert reply["ok"] and reply["result"]["consensus"] == "INDETERMINATE"
                weak = Consensus(config, bytes.fromhex(os.environ["DOG_CHAT_KEY"])).state()
                assert waiting["id"] not in weak.messages
                for peer in peers:
                    peer.start()
                assert mutate(waiting, sessions["alice"])["application"]["status"] == "APPLIED"
                checks.append("pérdida de seguidor, reelección y reintento sin confirmación minoritaria")
            else:
                directory = read("e2ee-directory", {"conversation": group}, "alice")["result"]
                _, aborted, _ = make_message("alice", group, directory)
                victim = bank.nodes["n3"]
                victim.kill()
                reply = client.call(aborted, sessions["alice"])
                assert reply["ok"] and reply["result"]["consensus"] in {"PENDING", "INDETERMINATE"}
                victim.start()
                assert mutate(aborted, sessions["alice"])["consensus"] == "ABORTED"
                rows = read("history", {"conversation": group, "after": "0", "limit": 100}, "alice")["result"]["messages"]
                assert all(row["id"] != aborted["id"] for row in rows)
                _, reply_id, _ = make_message("alice", group, directory)
                reached, release, _ = bank.pause("request", twopc.COMMIT, src=bank.coordinator)
                try:
                    reply = client.call(reply_id, sessions["alice"])
                    assert reply["ok"] and reached.wait(8), "No se observó el COMMIT durable"
                    bank.nodes[bank.coordinator].kill()
                finally:
                    release.set()
                bank.nodes[bank.coordinator].start()
                applied_result = mutate(reply_id, sessions["alice"])
                assert applied_result["application"]["status"] == "APPLIED"
                assert mutate(reply_id, sessions["alice"]) == applied_result
                checks.append("ABORT de envío cifrado sin participante y recuperación tras COMMIT durable")
            cursor = read("conversations", {}, "alice")["cursor"]
            link = prepare_link("linked", cursor)
            assert client.call(make("link-info", {"link": link["id"]}), sessions["bob"])["error"] == "UNKNOWN_LINK"
            cancellation = prepare_link("cancelled", cursor)
            assert mutate(make("link-cancel", {"link": cancellation["id"]}), sessions["cancelled"])["application"]["status"] == "APPLIED"
            assert client.call(approve(cancellation), sessions["alice"])["error"] == "LINK_USED"
            assert client.call(approve(link, signer="linked"), sessions["alice"])["error"] == "INVALID_DEVICE_PROOF"
            assert client.call(approve(link, timestamp=link["expires"]), sessions["alice"])["error"] == "LINK_EXPIRED"
        for node in bank.nodes.values():
            node.kill()
        for replica in replicas.values():
            replica.stop()
        shutil.rmtree(media[1] / "downloads")
        corrupted = replicas["n1"].root / (evidence[0]["ciphertext"] + ".bin")
        size = corrupted.stat().st_size
        corrupted.write_bytes(bytes(size))
        for node in bank.nodes.values():
            node.start()
        for replica in replicas.values():
            replica.start()
        if algorithm == "raft":
            eventually(lambda: bank.cluster.find_leader(.1), "Sin líder al reiniciar")
        with gateway(log, config, tls, ports, key, media=media):
            assert read("link-info", {"link": link["id"]}, "linked")["result"]["status"] == "PENDING"
            competing = [approve(link), approve(link)]
            def decide(request):
                def attempt():
                    reply = client.call(request, sessions["alice"])
                    if reply.get("error") == "LINK_USED":
                        return "LINK_USED"
                    value = active(reply)
                    if value:
                        return value["application"].get("reason", value["application"]["status"])
                return eventually(attempt, "Vínculo sin decisión", timeout=20)
            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = list(executor.map(decide, competing))
            assert sorted(outcomes) == ["APPLIED", "LINK_USED"], outcomes
            assert read("link-info", {"link": link["id"]}, "linked")["result"]["status"] == "LINKED"
            assert client.call(approve(link), sessions["alice"])["error"] == "LINK_USED"
            share_and_verify(direct, first_text)
            checks.append("QR PNG real leído, prueba incorrecta, cancelación, expiry, reinicio y consumo único concurrente")
            checks.append("segundo dispositivo recupera claves históricas protegidas y referencias sin duplicar")
            revoked_link = mutate(make("device-revoke", {"device": device["linked"]["id"]}), sessions["alice"])
            assert revoked_link["application"]["status"] == "APPLIED"
            assert client.call(make("devices"), sessions["linked"])["error"] == "AUTH_REQUIRED"
            for attachment, owner in stored:
                eventually(lambda: read("download-init", {"attachment": attachment}, owner)["result"]["status"] == "READY",
                           "La referencia cifrada no se recuperó", timeout=20)
            media_secret = bytes.fromhex(os.environ["DOG_MEDIA_KEY"])
            eventually(lambda: replicas["n1"].available(evidence[0]["ciphertext"], size, media_secret),
                       "No se reparó la copia cifrada", timeout=20)
            checks.append("copia DGF1 corrupta reparada desde otra réplica después de reiniciar")
            recovered = read("history", {"conversation": direct, "after": "0", "limit": 100}, "bob")
            assert web.call("open", "bob", box=recovered["result"]["messages"][0]["envelope"],
                            sender=device["alice"], meta=first_meta) == first_text
            cursor = int(recovered["cursor"])
            secret = bytes.fromhex(os.environ["DOG_CHAT_KEY"])
            hashes = {node: Consensus(config, secret, reader=node).state(cursor).public_digest() for node in bank.nodes}
            assert len(set(hashes.values())) == 1
        paths = [log] + list(root.rglob("*"))
        for path in paths:
            if path.is_file():
                data = path.read_bytes()
                assert not any(text in data for text in texts), f"Texto privado en {path.name}"
        checks.append("reinicio de gateway y réplicas, convergencia y ausencia del texto protegido en logs y datos")
        return {"algorithm": algorithm, "status": "PASS", "checks": checks, "hashes": hashes, "files": evidence}
    finally:
        for replica in replicas.values():
            replica.stop()
        bank.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--only", choices=("raft", "2pc"))
    args = parser.parse_args()
    args.export.mkdir(parents=True, exist_ok=False)
    os.environ["DOG_CHAT_KEY"] = secrets.token_hex(32)
    os.environ["DOG_MEDIA_KEY"] = secrets.token_hex(32)
    os.environ["DOG_MEDIA_RECEIPT_KEY"] = secrets.token_hex(32)
    with tempfile.TemporaryDirectory(prefix="dog-e2ee-") as temp, ExitStack() as stack:
        root = Path(temp)
        tls = root / "tls"
        create(tls, [HOST])
        jar = ROOT / "build/desktop/dog-messenger-desktop.jar"
        subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-cp", str(jar), "-d", temp,
                        str(ROOT / "clients/desktop/test/E2eeDriver.java")], check=True)
        java = Cryptography(stack, ["java", "-cp", os.pathsep.join([temp, str(jar)]), "E2eeDriver"])
        web = Cryptography(stack, ["node", str(ROOT / "clients/web/e2ee_driver.cjs")])
        results = []
        try:
            for algorithm in ([args.only] if args.only else ["raft", "2pc"]):
                results.append(run(root / algorithm, args.export, algorithm, tls, java, web))
                print(f"PASS E2EE {algorithm}", flush=True)
        finally:
            (args.export / "results.json").write_text(json.dumps({
                "platform": platform.platform(), "scenarios": results
            }, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
