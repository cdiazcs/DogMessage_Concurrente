"""Enviar o reintentar una transacción 2PC con su misma identidad."""
from __future__ import annotations

import argparse
from pathlib import Path
import time
import uuid

from cluster_lib import dm_node
from raft_cli import TEST_BYTES
from raft_config import load_config
import two_pc_wire as w

RESULTS = ("COMMITTED", "ABORTED", "PENDING", "CONFLICT", "NOT_COORDINATOR")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--client", default="console")
    parser.add_argument("--request-id", type=uuid.UUID)
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--start-node")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text")
    group.add_argument("--hex")
    group.add_argument("--binary-test", action="store_true")
    args = parser.parse_args()
    try:
        addresses = load_config(args.config, "2pc")
        coordinator = next(iter(addresses.values())).coordinator
        target = args.start_node or coordinator
        w.check(target in addresses and 1 <= args.attempts <= 100, "Nodo o cantidad de intentos inválido")
        data = args.text.encode("utf-8") if args.text is not None else bytes.fromhex(args.hex) if args.hex is not None else TEST_BYTES
        token = (args.request_id or uuid.uuid4()).bytes
        body = w.encode(w.SUBMIT, token, args.client, w.command_bytes(data))
    except (OSError, ValueError) as error:
        parser.error(str(error))

    client = dm_node.TcpClient(1)
    print(f"cliente={args.client} request_id={uuid.UUID(bytes=token)} bytes={len(data)}", flush=True)
    start = time.monotonic()
    for attempt in range(args.attempts):
        try:
            response = client.exchange(addresses[target].endpoint, body)
            status, sequence, leader = w.correlate(response, target, token, w.SUBMIT)
            w.check(leader == coordinator, "Coordinador devuelto no coincide con la configuración")
            print(f"nodo={target} {RESULTS[status]} sequence={sequence} elapsed_ms={(time.monotonic()-start)*1000:.1f}")
            if status in (w.COMMITTED, w.ABORTED, w.CONFLICT):
                return {w.COMMITTED: 0, w.ABORTED: 4, w.CONFLICT: 2}[status]
        except (OSError, ValueError) as error:
            print(f"nodo={target} intento={attempt + 1} ERROR {error}")
        target = coordinator
        if attempt + 1 < args.attempts:
            time.sleep(.15)
    print("Resultado desconocido o pendiente; reutiliza el mismo ID y comando.")
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
