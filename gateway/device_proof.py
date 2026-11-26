"""Verificación de prueba de posesión con el proveedor OpenSSL del host."""
from pathlib import Path
import subprocess
import tempfile

from e2ee_model import b64, identity


def der_signature(raw):
    def integer(value):
        encoded = value.lstrip(b"\0") or b"\0"
        if encoded[0] & 128:
            encoded = b"\0" + encoded
        return bytes((2, len(encoded))) + encoded
    body = integer(raw[:32]) + integer(raw[32:])
    return bytes((48, len(body))) + body


def verify(signing, proof, statement, exchange=None):
    signature = der_signature(b64(proof, 64))
    with tempfile.TemporaryDirectory(prefix="dog-proof-") as home:
        public, sig = Path(home) / "public.der", Path(home) / "proof.der"
        public.write_bytes(b64(signing, 91))
        sig.write_bytes(signature)
        try:
            if exchange is not None:
                point = Path(home) / "exchange.der"
                point.write_bytes(b64(exchange, 91))
                result = subprocess.run(["openssl", "pkey", "-pubin", "-inform", "DER", "-in", str(point),
                                         "-pubcheck", "-noout"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                        timeout=3, check=False)
                if result.returncode != 0:
                    return False
            result = subprocess.run(["openssl", "dgst", "-sha256", "-keyform", "DER",
                                     "-verify", str(public), "-signature", str(sig)],
                                    input=statement.encode("ascii"), stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL, timeout=3, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return False
        return result.returncode == 0


def verify_device(data, request, session):
    device = identity(data["exchange"], data["signing"])
    if device != data["device"]:
        return False
    statement = f"dog.device.1\n{request}\n{session}\n{device}"
    return verify(data["signing"], data["proof"], statement, data["exchange"])
