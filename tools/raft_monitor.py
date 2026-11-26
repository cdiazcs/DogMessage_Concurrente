"""Sondea DM1 y muestra los últimos estados observables en logs Raft opcionales."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import time

from cluster_lib import ROLE_LINE, dm_node
from raft_config import load_config

UNKNOWN = "DESCONOCIDO"
INDICES = re.compile(r"\b(commitIndex|lastApplied|lastLogIndex)=(\d+)\b")


def log_state(text, node_id):
    result = {}
    # Cada reinicio de los bancos añade un delimitador antes de abrir el listener.
    for line in text.rsplit("=== start ", 1)[-1].splitlines():
        if not line.startswith(f"[RAFT {node_id}] "):
            continue
        role = ROLE_LINE.match(line)
        if role:
            _, state, term, leader, _, _ = role.groups()
            if result.get("term") != term:
                result.pop("lastLogIndex", None)
            result.update(role=state, term=term, leader=leader if leader != "-" else UNKNOWN)
        result.update(INDICES.findall(line))
    if int(result.get("lastLogIndex", 0)) < int(result.get("commitIndex", 0)):
        # Una transición anterior puede haber registrado un tamaño previo al último commit.
        result.pop("lastLogIndex", None)
    return result


def observe(address, timeout, logs_dir):
    try:
        reply = dm_node.TcpClient(timeout).ping(address.endpoint, "monitor")
        if reply != address.id:
            raise ValueError(f"El servidor respondió como {reply}, se esperaba {address.id}")
    except (OSError, ValueError) as error:
        return False, {}, str(error)
    if logs_dir is None:
        return True, {}, None
    path = logs_dir / f"{address.id}-{address.language}.log"
    try:
        return True, log_state(path.read_text(encoding="utf-8-sig", errors="replace"), address.id), None
    except OSError as error:
        return True, {}, f"estado del log no disponible: {error}"


def render(addresses, timeout, logs_dir=None):
    with ThreadPoolExecutor(max_workers=min(len(addresses), 16)) as executor:
        state = list(executor.map(lambda address: observe(address, timeout, logs_dir), addresses.values()))
    count = 0
    leaders = []
    for address, (reachable, fields, error) in zip(addresses.values(), state):
        count += reachable
        if fields.get("role") == "LEADER":
            leaders.append(f"{address.id}@{fields['term']}")
        if address.coordinator:
            role = "COORDINATOR" if address.id == address.coordinator else "PARTICIPANT"
            print(f"{address.id} {address.language} {address.host}:{address.port} "
                  f"{'ALCANZABLE' if reachable else 'NO DISPONIBLE'} rol_configurado={role} "
                  f"coordinador={address.coordinator} transacciones={UNKNOWN}")
            if error and not reachable:
                print(f"  {error}")
            continue
        summary = " ".join(f"{key}={fields.get(key, UNKNOWN)}"
                           for key in ("role", "term", "leader", "commitIndex", "lastApplied", "lastLogIndex"))
        print(f"{address.id} {address.language} {address.host}:{address.port} "
              f"{'ALCANZABLE' if reachable else 'NO DISPONIBLE'} {summary}")
        if error:
            print(f"  {error}")
    if any(address.coordinator for address in addresses.values()):
        print(f"alcanzables={count}/{len(addresses)} estado_atómico={UNKNOWN}; inspecciona con nodos detenidos")
        return count
    print(f"alcanzables={count}/{len(addresses)} mayoría_configurada={len(addresses) // 2 + 1} "
          f"quórum_confirmado={UNKNOWN}")
    if logs_dir is not None:
        print("Estado Raft según la última observación de los logs locales; puede estar desactualizado.")
    if leaders:
        print("Líderes observados (nodo@término): " + ", ".join(leaders))
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--logs-dir", type=Path)
    parser.add_argument("--timeout", type=float, default=1)
    parser.add_argument("--watch", type=float, metavar="SEGUNDOS")
    args = parser.parse_args()
    try:
        addresses = load_config(args.config, algorithm=None)
        if not 0 < args.timeout <= 5 or (args.watch is not None and not 0 < args.watch <= 3600):
            raise ValueError("Intervalo o timeout fuera de rango")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    try:
        while True:
            render(addresses, args.timeout, args.logs_dir)
            if args.watch is None:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
