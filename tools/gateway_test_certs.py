"""Genera una CA local y un certificado de desarrollo con OpenSSL."""
import argparse
import ipaddress
from pathlib import Path
import re
import subprocess


def create(path, names):
    addresses = []
    for name in names:
        try:
            addresses.append("IP:" + str(ipaddress.ip_address(name)))
        except ValueError:
            if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,252})", name):
                raise ValueError("Nombre SAN inválido")
            addresses.append("DNS:" + name)
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    (path / "server.ext").write_text("basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
        f"subjectAltName={','.join(addresses)}\n", encoding="ascii")

    def openssl(*args):
        subprocess.run(["openssl", *args], cwd=path, check=True, capture_output=True)

    openssl("req", "-x509", "-newkey", "rsa:3072", "-nodes", "-days", "30",
            "-subj", "/CN=Dog Messenger Test CA", "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", "ca.key", "-out", "ca.pem")
    openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=Dog Messenger Test Gateway",
            "-keyout", "server.key", "-out", "server.csr")
    openssl("x509", "-req", "-days", "14", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-CAcreateserial", "-extfile", "server.ext", "-out", "server.pem")
    (path / "ca.key").chmod(0o600)
    (path / "server.key").chmod(0o600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--san", action="append", default=["localhost", "127.0.0.1", "10.0.2.2"])
    args = parser.parse_args()
    create(args.directory, args.san)
    print(f"CA pública de ensayo: {args.directory / 'ca.pem'}")
