"""Envía un CLIENT_COMMAND DM2 y conserva su identidad durante los reintentos."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import time
import uuid

from cluster_lib import dm2, dm_node
from raft_config import load_config

STATUSES = {dm2.COMMITTED: "COMMITTED", dm2.NOT_LEADER: "NOT_LEADER",
            dm2.RETRY: "RETRY", dm2.REQUEST_ID_CONFLICT: "REQUEST_ID_CONFLICT"}
TEST_BYTES = bytes(range(256))


def send(addresses, message, attempts, timeout, start_node=None, emit=print):
    client = dm_node.TcpClient(timeout)
    ordered = list(addresses)
    index = ordered.index(start_node) if start_node else 0
    start = time.monotonic()
    for attempt in range(1, attempts + 1):
        target = ordered[index]
        endpoint = addresses[target].endpoint
        next_index = (index + 1) % len(ordered)
        try:
            result = client.call(endpoint, target, message).payload
            if isinstance(result, dm2.ClientResult):
                hint = result.leader_hint
                emit(f"intento={attempt} nodo={target} {STATUSES[result.status]}" +
                     (f" líder={hint}" if hint else ""))
                if result.status in (dm2.COMMITTED, dm2.REQUEST_ID_CONFLICT):
                    return target, result, time.monotonic() - start
                if hint in addresses and hint != target:
                    next_index = ordered.index(hint)
            else:
                emit(f"intento={attempt} nodo={target} ERROR DM2 código={result.code}")
        except OSError as error:
            emit(f"intento={attempt} nodo={target} ERROR {error}")
        index = next_index
        if attempt < attempts:
            time.sleep(.05)
    return None, None, time.monotonic() - start


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--client", default="console")
    parser.add_argument("--request-id", type=uuid.UUID)
    parser.add_argument("--attempts", type=int, default=9)
    parser.add_argument("--timeout", type=float, default=2)
    parser.add_argument("--start-node", help="Nodo al que se envía el primer intento")
    payload = parser.add_mutually_exclusive_group(required=True)
    payload.add_argument("--text")
    payload.add_argument("--hex", dest="hex_bytes")
    payload.add_argument("--binary-test", action="store_true")
    args = parser.parse_args()
    try:
        addresses = load_config(args.config)
        if not 1 <= args.attempts <= 100 or not 0 < args.timeout <= 5:
            raise ValueError("--attempts debe estar entre 1 y 100 y --timeout en (0, 5]")
        if args.start_node is not None and args.start_node not in addresses:
            raise ValueError("--start-node debe pertenecer al clúster")
        command = (args.text.encode("utf-8") if args.text is not None else
                   bytes.fromhex(args.hex_bytes) if args.hex_bytes is not None else TEST_BYTES)
        token = args.request_id if args.request_id is not None else uuid.uuid4()
        message = dm2.Message(dm2.Header(dm2.RAFT, dm2.CLIENT_COMMAND, token.bytes, args.client),
                              dm2.ClientCommand(command))
    except (ValueError, OSError) as error:
        parser.error(str(error))
    print(f"cliente={args.client} request_id={token.hex} bytes={len(command)}", flush=True)
    target, result, elapsed = send(addresses, message, args.attempts, args.timeout, args.start_node,
                                   emit=lambda line: print(line, file=sys.stderr, flush=True))
    if result is None:
        print(f"INDETERMINADO elapsed_ms={elapsed * 1000:.1f}. Reintenta con el mismo cliente, ID y bytes.")
        return 3
    if result.status == dm2.REQUEST_ID_CONFLICT:
        print(f"REQUEST_ID_CONFLICT nodo={target}")
        return 2
    print(f"COMMITTED nodo={target} index={result.log_index} elapsed_ms={elapsed * 1000:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
