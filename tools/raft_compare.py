"""Compara inspecciones de nodos detenidos y calcula SHA-256 canónico."""
from __future__ import annotations

import argparse
import hashlib
from itertools import combinations
import json
from pathlib import Path
import re

from cluster_lib import StepFailed
from trilingual_cluster_test import canonical, compare_prefix


def inspect_file(path):
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    # Los lanzadores pueden imprimir el resultado de compilación antes de --inspect.
    headers = [line for line in lines if line.startswith("role=")]
    if len(headers) != 1:
        raise ValueError(f"{path}: se esperaba una salida de --inspect")
    commit = re.search(r"\bcommitIndex=(\d+)\b", headers[0])
    entries = canonical(lines)
    if commit is None or int(commit.group(1)) != len(entries):
        raise ValueError(f"{path}: commitIndex no coincide con el prefijo")
    return entries


def compare_files(paths):
    logs = {}
    for path in paths:
        entries = inspect_file(path)
        encoded = (json.dumps(entries, separators=(",", ":")) + "\n").encode()
        no_ops = sum(e[2] == "NO_OP" for e in entries)
        print(f"{path} comandos={len(entries)-no_ops} NO_OP={no_ops} "
              f"commitIndex={len(entries)} SHA-256={hashlib.sha256(encoded).hexdigest()}")
        logs[path] = entries
    divergent = False
    lag = False
    for (left, a), (right, b) in combinations(logs.items(), 2):
        shared = min(len(a), len(b))
        try:
            compare_prefix(a[:shared], b[:shared], str(right))
        except StepFailed as error:
            print(f"DIVERGENCIA {left} frente a {error}")
            divergent = True
            continue
        if len(a) != len(b):
            print(f"PREFIJO COMPARTIDO {left} y {right} hasta índice {shared}; "
                  f"menor commitIndex en {left if len(a) < len(b) else right}")
            lag = True
    if not divergent and not lag:
        print("PREFIJOS COMPROMETIDOS IDÉNTICOS")
    return int(divergent)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inspections", nargs="+", type=Path)
    args = parser.parse_args()
    if len(set(args.inspections)) < 2:
        parser.error("Proporciona al menos dos inspecciones distintas")
    try:
        return compare_files(args.inspections)
    except (OSError, ValueError, StepFailed) as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
