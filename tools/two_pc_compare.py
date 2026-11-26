"""Compara decisiones 2PC de inspecciones en frío y verifica el historial aplicado."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import re

ROW = re.compile(r"sequence=(\d+) status=(STARTED|PREPARED|COMMIT|ABORT) "
                 r"tx=([0-9a-f-]+) client=([A-Za-z0-9_-]+) requestId=([0-9a-f-]+) command=([0-9a-f]+)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", type=Path, nargs="+")
    args = parser.parse_args()
    if len(args.files) < 2:
        parser.error("Indica al menos dos inspecciones")
    observed = {}
    try:
        for path in args.files:
            lines = [line for line in path.read_text(encoding="utf-8-sig").splitlines() if line.startswith("sequence=")]
            parsed = [ROW.fullmatch(line) for line in lines]
            if any(match is None for match in parsed):
                raise ValueError(f"Inspección 2PC inválida: {path}")
            applied = [line for line, match in zip(lines, parsed) if match[2] == "COMMIT"]
            aborted = sum(match[2] == "ABORT" for match in parsed)
            pending = len(lines) - len(applied) - aborted
            digest = hashlib.sha256(("\n".join(applied) + "\n").encode()).hexdigest()
            print(f"{path} COMMIT={len(applied)} ABORT={aborted} PENDING={pending} SHA-256={digest}")
            for seq, match in enumerate(parsed, 1):
                if int(match[1]) != seq:
                    raise ValueError(f"{path}: hueco en la secuencia {seq}")
                other = observed.get(seq)
                identity = match.groups()[2:]
                if other is not None:
                    old_identity, old_status = other
                    if old_identity != identity or (old_status in ("COMMIT", "ABORT") and match[2] in ("COMMIT", "ABORT")
                                                     and old_status != match[2]):
                        print(f"DIVERGENCIA en secuencia {seq}")
                        return 1
                if other is None or match[2] in ("COMMIT", "ABORT"):
                    observed[seq] = (identity, match[2])
        print("Sin decisiones contradictorias en las secuencias observadas")
        return 0
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
