"""Banco de acceso DOG/1 con TLS, procesos reales y configuraciones independientes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import http.client
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import struct
import sys
import subprocess
import tempfile
import time
import uuid

from chat_cluster_test import write_config
from chat_service import Consensus
from cluster_lib import HOST, StepFailed
from gateway_test_certs import create
from trilingual_cluster_test import Round, eventually, reserve_ports
from two_phase_commit_test import Bank

ROOT = Path(__file__).resolve().parents[1]


def make(op, data=None):
    return {"v": 1, "id": uuid.uuid4().hex, "op": op, "data": data or {}}


def exact(conn, size):
    result = bytearray()
    while len(result) < size:
        data = conn.recv(size - len(result))
        if not data:
            raise EOFError("Respuesta incompleta")
        result.extend(data)
    return result


class TLSClient:
    def __init__(self, ports, ca):
        self.tcp, self.web = ports
        self.context = ssl.create_default_context(cafile=str(ca))

    def wrap(self, context=None, identity=HOST):
        tcp = socket.create_connection((HOST, self.tcp), timeout=15)
        try:
            return (context or self.context).wrap_socket(tcp, server_hostname=identity)
        except BaseException:
            tcp.close()
            raise

    def call(self, req, token=None, web=False):
        if web:
            headers = {"Content-Type": "application/json", "Origin": f"https://{HOST}:{self.web}"}
            if token is not None:
                headers["Authorization"] = "Bearer " + token
            with_https = http.client.HTTPSConnection(HOST, self.web, context=self.context, timeout=15)
            try:
                with_https.request("POST", "/api/v1", json.dumps(req), headers)
                response = with_https.getresponse()
                assert response.status == 200
                reply = json.loads(response.read())
            finally:
                with_https.close()
        else:
            data = json.dumps(req if token is None else dict(req, session=token)).encode()
            with self.wrap() as conn:
                conn.sendall(struct.pack(">I", len(data)) + data)
                reply = json.loads(exact(conn, struct.unpack(">I", exact(conn, 4))[0]))
        assert reply["v"] == 1 and reply["id"] == req["id"]
        return reply


@contextmanager
def gateway(path, config, tls, ports, key, clear=False, media=None):
    tcp, web = ports
    env = {**os.environ, "DOG_GATEWAY_KEY": key}
    env.pop("DOG_MEDIA_RECEIPT_KEY", None)
    args = [sys.executable, str(ROOT / "gateway/chat_gateway.py"),
            "--config", str(config), "--tcp", str(tcp), "--web", str(web),
            "--origin", f"{'http' if clear else 'https'}://{HOST}:{web}"]
    args += ["--loopback-cleartext"] if clear else ["--cert", str(tls / "server.pem"), "--key", str(tls / "server.key")]
    if media is not None:
        args += ["--media-config", str(media[0]), "--media-data", str(media[1])]
    with path.open("ab") as stream:
        process = subprocess.Popen(args, env=env, stdout=stream, stderr=subprocess.STDOUT)

        def listening():
            if process.poll() is not None:
                raise RuntimeError("El gateway terminó al iniciar")
            try:
                with socket.create_connection((HOST, web), timeout=.2):
                    return True
            except OSError:
                return False

        try:
            eventually(listening, "El gateway no escuchó")
            yield
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)


def applied(reply):
    assert reply["ok"], reply.get("error")
    result = reply["result"]
    assert result["consensus"] == "COMMITTED" and result["application"]["status"] == "APPLIED", result
    return result


def check_interfaces(root, log, config, tls, ports, key, evidence, browser, media=None):
    java_home = root / "ui"
    java_home.mkdir()
    trust = java_home / "trust.jks"
    jar = ROOT / "build/desktop/dog-messenger-desktop.jar"
    subprocess.run(["keytool", "-importcert", "-noprompt", "-alias", "dog-test-ca", "-file",
                    str(tls / "ca.pem"), "-keystore", str(trust), "-storetype", "JKS",
                    "-storepass", "changeit"], check=True, capture_output=True)
    subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-cp", str(jar),
                    "-d", str(java_home), str(ROOT / "clients/desktop/test/DesktopSmoke.java")], check=True)
    desktop = ["java", "-Djavax.net.ssl.trustStore=" + str(trust), "-cp",
               os.pathsep.join((str(java_home), str(jar))), "DesktopSmoke", str(ports[0])]
    with (evidence / "swing.log").open("ab") as output:
        subprocess.run(desktop, check=True, stdout=output, stderr=subprocess.STDOUT, timeout=55)
    local_ports = reserve_ports(2)
    # El ensayo web comparte la sesión de consenso. Su staging es independiente.
    browser_media = (media[0], root / "web-staging") if media else None
    web_files = [str((evidence / "pixel.png").resolve())] if media else []
    with gateway(log, config, tls, local_ports, key, clear=True, media=browser_media):
        with (evidence / "chrome.log").open("ab") as output:
            subprocess.run(["node", str(ROOT / "clients/web/smoke.cjs"), browser,
                            f"http://{HOST}:{local_ports[1]}", *web_files], check=True,
                           stdout=output, stderr=subprocess.STDOUT, timeout=45)
    downloaded = root / "swing-descarga.png"
    swing_files = [str((evidence / "pagina.pdf").resolve()), str(downloaded)] if media else []
    with (evidence / "swing.log").open("ab") as output:
        subprocess.run([*desktop, "receive", *swing_files], check=True, stdout=output,
                       stderr=subprocess.STDOUT, timeout=55)
    if media:
        browser_downloads = root / "browser-download"
        browser_downloads.mkdir()
        with gateway(log, config, tls, local_ports, key, clear=True, media=browser_media):
            with (evidence / "chrome.log").open("ab") as output:
                subprocess.run(["node", str(ROOT / "clients/web/smoke.cjs"), browser,
                                f"http://{HOST}:{local_ports[1]}", "--receive", str(browser_downloads)],
                               check=True, stdout=output, stderr=subprocess.STDOUT, timeout=45)
            expected_pdf = (evidence / "pagina.pdf").read_bytes()
            def browser_saved():
                file = browser_downloads / "pagina.pdf"
                return file.is_file() and file.read_bytes() == expected_pdf
            eventually(browser_saved, "Chrome no guardó el PDF verificado")
    client = TLSClient(ports, tls / "ca.pem")
    login = make("login", {"username": "web_test", "password": "solo-prueba-web-2026"})
    token = applied(client.call(login, web=True))["session"]
    items = client.call(make("conversations"), token, web=True)["result"]["items"]
    for item in items:
        if item["type"] == "DIRECT":
            history = make("history", {"conversation": item["id"], "after": "0", "limit": 20})
            rows = client.call(history, token, web=True)["result"]["messages"]
            texts = [message["text"] for message in rows if message["type"] == "TEXT"]
            if texts == ["<script>throw 123</script> 🐶", "Respuesta de Swing 🐕"]:
                if media:
                    assert downloaded.read_bytes() == (evidence / "pixel.png").read_bytes()
                    files = {row["attachment"]["filename"]: row["attachment"] for row in rows if "attachment" in row}
                    assert {"pixel.png", "pagina.pdf"} <= set(files)
                    pdf = files["pagina.pdf"]
                    def ready():
                        reply = client.call(make("download-init", {"attachment": pdf["id"]}), token, web=True)
                        return reply["ok"] and reply["result"]["status"] == "READY"
                    eventually(ready, "PDF Swing sin copia verificable")
                    req = make("download-chunk", {"attachment": pdf["id"], "offset": 0})
                    raw = client.call(req, token, web=True)["result"]["block"]
                    import base64
                    assert base64.b64decode(raw) == (evidence / "pagina.pdf").read_bytes()
                return
    raise AssertionError("Intercambio entre interfaces no observado")


def run(root, algorithm, tls, evidence, browser):
    root.mkdir()
    bank = Round(root / "nodes", reserve_ports(3), False) if algorithm == "raft" else Bank(root / "nodes", "n1")
    for node in bank.nodes.values():
        node.chat_feed = True
    config = root / "cluster.json"
    write_config(config, bank, algorithm)
    ports = reserve_ports(2)
    key = secrets.token_hex(32)
    client = TLSClient(ports, tls / "ca.pem")
    log = evidence / (algorithm + "-gateway.log")
    events, protected = [], [key, os.environ["DOG_CHAT_KEY"]]
    if browser:
        protected.extend(("solo-prueba-swing-2026", "solo-prueba-web-2026"))

    def call(op, data=None, token=None, web=False):
        return client.call(make(op, data), token, web)

    try:
        for node in bank.nodes.values():
            node.start()
        if algorithm == "raft":
            eventually(lambda: bank.cluster.find_leader(.1), "Sin líder")
        with gateway(log, config, tls, ports, key):
            for context, name in ((ssl.create_default_context(), HOST), (client.context, "otro.invalid")):
                try:
                    client.wrap(context, name).close()
                except ssl.SSLCertVerificationError:
                    pass
                else:
                    raise AssertionError("TLS aceptó una identidad no verificada")
            events.append("CA verificada y certificados no confiables o con SAN incorrecto rechazados")
            with client.wrap() as conn:
                conn.sendall(struct.pack(">I", 16_385))
                assert conn.recv(1) == b""
            with client.wrap() as conn:
                data = b'{"v":1,"v":1}'
                conn.sendall(struct.pack(">I", len(data)) + data)
                assert json.loads(exact(conn, struct.unpack(">I", exact(conn, 4))[0]))["error"] == "INVALID_JSON"
            with client.wrap() as conn:
                started = time.monotonic()
                conn.sendall(struct.pack(">I", 2) + b"{")
                conn.settimeout(9)
                assert conn.recv(1) == b"" and 4.5 <= time.monotonic() - started < 9
            https = http.client.HTTPSConnection(HOST, ports[1], context=client.context, timeout=10)
            for method, path, expected in (("GET", "/", 200), ("GET", "/../server.key", 404),
                                           ("GET", "/feed", 404), ("PATCH", "/api/v1", 405)):
                https.request(method, path)
                response = https.getresponse()
                assert response.status == expected
                response.read()
            https.request("POST", "/api/v1", "{}", {"Content-Type": "application/json",
                                                   "Origin": "https://otro.invalid"})
            assert https.getresponse().status == 403
            https.close()
            assert call("users")["error"] == "AUTH_REQUIRED"
            assert call("login", {"username": "noexiste", "password": "pass-no-valida"})["error"] == "INVALID_CREDENTIALS"
            events.append("rutas, origen, entrada malformada, límite y timeout TCP, feed privado y autenticación")
            users, tokens = [], []
            for i in range(3):
                password = secrets.token_hex(14)
                protected.append(password)
                username = f"client{i}"
                register = make("register", {"username": username, "display": f"Persona {i}", "password": password})
                registration = applied(client.call(register, web=i == 1))
                users.append(registration["application"]["user"])
                assert applied(client.call(register, web=i == 1)) == registration
                login = make("login", {"username": username, "password": password})
                signed = applied(client.call(login, web=i == 1))
                assert applied(client.call(login, web=i == 1)) == signed
                tokens.append(signed["session"])
                protected.append(signed["session"])
            direct = applied(call("direct", {"peer": users[1]}, tokens[0]))["application"]["conversation"]
            history = {"conversation": direct, "after": "0", "limit": 1}
            assert call("history", history, tokens[2], True)["error"] == "FORBIDDEN"
            first = make("send", {"conversation": direct, "text": "Texto 🐕 <script>"})
            initial = applied(client.call(first, tokens[0]))
            assert applied(client.call(first, tokens[0])) == initial
            assert client.call(dict(first, data={**first["data"], "text": "distinto"}), tokens[0])["result"]["consensus"] == "CONFLICT"
            second = make("send", {"conversation": direct, "text": "Respuesta"})
            applied(client.call(second, tokens[1], True))
            page = call("history", history, tokens[1], True)["result"]
            assert page["more"] and [x["text"] for x in page["messages"]] == [first["data"]["text"]]
            sync = {**history, "after": page["next_cursor"]}
            assert [x["text"] for x in call("sync", sync, tokens[0])["result"]["messages"]] == ["Respuesta"]
            assert call("status", {"request": first["id"]}, tokens[0])["result"]["status"] == "APPLIED"
            assert call("status", {"request": first["id"]}, tokens[1])["error"] == "FORBIDDEN"
            group = applied(call("group-create", {"name": "Equipo", "members": [users[1]]},
                                 tokens[0]))["application"]["conversation"]
            member = {"conversation": group, "member": users[2]}
            assert call("group-add", member, tokens[1])["result"]["application"]["reason"] == "ADMIN_REQUIRED"
            applied(call("group-add", member, tokens[0]))
            def group_send(i):
                request = make("send", {"conversation": group, "text": f"grupo {i}"})

                def attempt():
                    response = client.call(request, tokens[i], web=i == 1)
                    if response.get("result", {}).get("consensus") in {"PENDING", "INDETERMINATE"}:
                        return None
                    return applied(response)

                return eventually(attempt, "Envío concurrente sin resolución", timeout=15)

            with ThreadPoolExecutor(max_workers=3) as executor:
                outcomes = list(executor.map(group_send, range(3)))
            assert len({result["position"] for result in outcomes}) == 3
            group_data = {"conversation": group, "after": "0", "limit": 40}
            views = [call("sync", group_data, token, True)["result"]["messages"] for token in tokens]
            assert len(views[0]) == 3 and views[0] == views[1] == views[2]
            applied(call("group-remove", member, tokens[0]))
            assert call("sync", group_data, tokens[2])["error"] == "FORBIDDEN"
            events.append("registro y login idempotentes, chat bilateral, paginación, grupos y concurrencia TCP/HTTPS")
            if algorithm == "raft":
                leader = bank.cluster.find_leader()
                follower = next(node for node in bank.nodes.values() if node is not leader)
                follower.kill()
                applied(call("send", {"conversation": direct, "text": "con mayoría"}, tokens[0]))
                follower.start()
                leader.kill()
                eventually(lambda: bank.cluster.find_leader(.1, exclude=[leader.node_id]), "Sin sucesor")
                applied(call("send", {"conversation": direct, "text": "nuevo líder"}, tokens[0]))
                leader.start()
                current = eventually(lambda: bank.cluster.find_leader(.1), "Sin líder para prueba de quórum")
                peers = [node for node in bank.nodes.values() if node is not current]
                for peer in peers:
                    peer.kill()
                minority = make("send", {"conversation": direct, "text": "reintento tras perder la mayoría"})
                assert client.call(minority, tokens[0])["result"]["consensus"] == "INDETERMINATE"
                assert call("history", history, tokens[0])["error"] == "AUTHORIZATION_UNAVAILABLE"
                for peer in peers:
                    peer.start()
                result = eventually(lambda: (reply if (reply := client.call(minority, tokens[0]))["result"]
                                    ["consensus"] == "COMMITTED" else None), "No se recuperó el quórum")
                assert applied(result)["application"]["message"] == minority["id"]
                assert applied(client.call(minority, tokens[0])) == applied(result)
                events.append("caída de seguidor, reelección, pérdida de mayoría y reintento seguro")
            else:
                participant = bank.nodes["n3"]
                participant.kill()
                pending = make("send", {"conversation": direct, "text": "abortada"})
                result = client.call(pending, tokens[0])["result"]
                assert result["consensus"] in {"PENDING", "INDETERMINATE"}
                participant.start()
                result = eventually(lambda: (reply if (reply := client.call(pending, tokens[0])["result"])
                                    ["consensus"] == "ABORTED" else None), "Sin ABORT")
                assert result["consensus"] == "ABORTED"
                bank.nodes["n1"].kill()
                assert client.call(second, tokens[1])["result"]["consensus"] in {"PENDING", "INDETERMINATE"}
                bank.nodes["n1"].start()
                assert applied(client.call(second, tokens[1]))["application"]["message"] == second["id"]
                events.append("ABORT tras caída de participante y recuperación tras caída de coordinador")

        with gateway(log, config, tls, ports, key):
            assert applied(client.call(first, tokens[0])) == initial
            recovered = call("sync", group_data, tokens[0], True)["result"]["messages"]
            assert recovered == views[0]
            applied(call("logout", None, tokens[2]))
            assert call("users", None, tokens[2])["error"] == "AUTH_REQUIRED"
            events.append("desconexión, reinicio del gateway, cursor, reintento y revocación")
            position = int(call("conversations", None, tokens[0])["cursor"])
            hashes = {node: Consensus(config, bytes.fromhex(os.environ["DOG_CHAT_KEY"]), reader=node)
                      .state(position).public_digest() for node in bank.nodes}
            assert len(set(hashes.values())) == 1
            if browser:
                check_interfaces(root, log, config, tls, ports, key, evidence, browser)
                events.append("intercambio Swing TLS y Chrome headless HTTP de ensayo")
            for node in bank.nodes.values():
                node.kill()
            for node in bank.nodes.values():
                node.start()
            if algorithm == "raft":
                eventually(lambda: bank.cluster.find_leader(.1), "No hubo líder tras reiniciar")
            assert call("sync", group_data, tokens[0])["result"]["messages"] == views[0]
            events.append("historial recuperado tras reiniciar las tres réplicas")
        assert all(value not in log.read_text(encoding="utf-8") for value in protected)
        return {"algorithm": algorithm, "status": "PASS", "checks": events, "hashes": hashes}
    finally:
        try:
            for node in bank.nodes.values():
                node.stop()
            for node in bank.nodes.values():
                if node.log_path.exists():
                    source = node.log_path.read_text(encoding="utf-8", errors="replace")
                    if any(value in source for value in protected):
                        raise AssertionError("Secreto encontrado en log del nodo")
                    shutil.copyfile(node.log_path, evidence / f"{algorithm}-{node.node_id}.log")
        finally:
            bank.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--browser", help="Ejecutable Chrome. Ejecuta pruebas Swing y web")
    parser.add_argument("--only", choices=("raft", "2pc"))
    args = parser.parse_args()
    args.export.mkdir(parents=True, exist_ok=False)
    os.environ["DOG_CHAT_KEY"] = secrets.token_hex(32)
    with tempfile.TemporaryDirectory(prefix="dog-gateway-") as home:
        root = Path(home)
        tls = root / "tls"
        create(tls, [HOST])
        results = []
        try:
            for algorithm in ([args.only] if args.only else ["raft", "2pc"]):
                try:
                    result = run(root / algorithm, algorithm, tls, args.export, args.browser)
                except (AssertionError, OSError, RuntimeError, StepFailed, subprocess.SubprocessError):
                    results.append({"algorithm": algorithm, "status": "FAIL"})
                    raise
                results.append(result)
                print(f"PASS {algorithm}", flush=True)
        finally:
            (args.export / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
