"""Intercambio E2EE real entre Swing y Chromium con CA instalada en un perfil temporal."""
from argparse import ArgumentParser
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import tempfile

from chat_cluster_test import write_config
from chromium_test_trust import configure
from cluster_lib import HOST
from gateway_cluster_test import gateway
from gateway_test_certs import create
from media_cluster_test import Replica, fixtures
from trilingual_cluster_test import Round, eventually, reserve_ports

ROOT = Path(__file__).resolve().parents[1]


def wait_public(proc, log, prefix):
    def read():
        if proc.poll() is not None:
            raise RuntimeError(f"El proceso terminó. Revisa {log}")
        found = re.search(r"^" + prefix + r" ([0-9a-f]{64})$", log.read_text(encoding="utf-8"), re.MULTILINE)
        return found.group(1) if found else None
    return eventually(read, f"No se observó la huella pública. Revisa {log}", timeout=32)


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--browser", required=True)
    parser.add_argument("--export", type=Path, required=True)
    args = parser.parse_args()
    args.export.mkdir(parents=True, exist_ok=False)
    os.environ["DOG_CHAT_KEY"] = secrets.token_hex(32)
    os.environ["DOG_MEDIA_KEY"] = secrets.token_hex(32)
    os.environ["DOG_MEDIA_RECEIPT_KEY"] = secrets.token_hex(32)
    with tempfile.TemporaryDirectory(prefix="dog-e2ee-ui-") as work, ExitStack() as stack:
        root = Path(work)
        tls = root / "tls"
        create(tls, [HOST])
        home = root / "browser-home"
        configure(home, tls / "ca.pem")
        trust = root / "java.jks"
        subprocess.run(["keytool", "-importcert", "-noprompt", "-alias", "dog-ca", "-file",
                        str(tls / "ca.pem"), "-keystore", str(trust), "-storetype", "JKS", "-storepass",
                        "changeit"], capture_output=True, check=True)
        jar = ROOT / "build/desktop/dog-messenger-desktop.jar"
        subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-cp", str(jar), "-d", work,
                        str(ROOT / "clients/desktop/test/SwingE2eeSmoke.java")], check=True)
        bank = Round(root / "nodes", reserve_ports(3), False)
        stack.callback(bank.close)
        for node in bank.nodes.values():
            node.chat_feed = True
            node.start()
        eventually(lambda: bank.cluster.find_leader(.1), "Clúster sin líder")
        config = root / "cluster.json"
        write_config(config, bank, "raft")
        storage = root / "stores"
        storage.mkdir()
        replicas = {node: Replica(storage, node, port) for node, port in zip(bank.nodes, reserve_ports(3))}
        for replica in replicas.values():
            stack.callback(replica.stop)
            replica.start()
        media_cfg = root / "media.json"
        media_cfg.write_text(json.dumps({"algorithm": "raft", "nodes": {
            name: [HOST, replica.port] for name, replica in replicas.items()}}))
        synthetic = root / "synthetic"
        synthetic.mkdir()
        files = fixtures()
        for name in ("pixel.png", "pagina.pdf"):
            (synthetic / name).write_bytes(files[name])
        downloads = root / "downloads"
        downloads.mkdir()
        tcp, web = reserve_ports(2)
        stack.enter_context(gateway(args.export / "gateway.log", config, tls, (tcp, web), secrets.token_hex(32),
                                    media=(media_cfg, root / "gateway-data")))

        def spawn(log, argv, env=None):
            with log.open("w", encoding="utf-8") as out:
                child = subprocess.Popen(argv, stdout=out, stderr=subprocess.STDOUT, env=env)
            def stop():
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=6)
            stack.callback(stop)
            return child

        public = root / "browser-fingerprint.txt"
        swing_path = args.export / "swing.log"
        swing = spawn(swing_path, ["java", "-Djavax.net.ssl.trustStore=" + str(trust),
            "-Ddog.e2ee.home=" + str(root / "vault"), "-cp", os.pathsep.join([work, str(jar)]),
            "SwingE2eeSmoke", str(tcp), str(public), str(synthetic / "pagina.pdf"), str(downloads / "pixel.png")])
        mark = wait_public(swing, swing_path, "PUBLIC_SWING")
        web_path = args.export / "chrome.log"
        chrome = spawn(web_path, ["node", str(ROOT / "clients/web/smoke.cjs"), args.browser,
            f"https://{HOST}:{web}", "--e2ee", mark, str(synthetic / "pixel.png"), str(downloads)], {**os.environ, "DOG_CHROME_HOME": str(home)})
        public.write_text(wait_public(chrome, web_path, "PUBLIC_BROWSER"), encoding="ascii")
        if swing.wait(timeout=65) or chrome.wait(timeout=65):
            raise RuntimeError("Falló el intercambio gráfico. Revisa los logs")
        assert "PASS Swing" in swing_path.read_text(encoding="utf-8")
        assert "PASS Chrome HTTPS" in web_path.read_text(encoding="utf-8")
        protected = (b"Texto E2EE desde Chrome", b"Texto E2EE desde Swing")
        for path in (root / "nodes").rglob("*"):
            if path.is_file():
                assert not any(text in path.read_bytes() for text in protected)
        for name in ("pixel.png", "pagina.pdf"):
            assert (downloads / name).read_bytes() == files[name]
        for path in storage.rglob("*.bin"):
            assert all(files[name] not in path.read_bytes() for name in ("pixel.png", "pagina.pdf"))
        print("PASS Swing y Chromium intercambiaron texto, PNG y PDF cifrados mediante TLS y HTTPS", flush=True)


if __name__ == "__main__":
    main()
