"""Exporta resultados, logs e inspecciones Raft sin copiar directorios persistentes."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import shutil


def export(source: Path, destination: Path):
    source = source.resolve()
    destination = destination.resolve()
    if source == destination or source in destination.parents:
        raise ValueError("El destino debe estar fuera del directorio de origen")
    report = json.loads((source / "results.json").read_text(encoding="utf-8"))
    scenarios = report.get("scenarios", report.get("cases")) if isinstance(report, dict) else None
    if not isinstance(scenarios, list) or not all(isinstance(s, dict) for s in scenarios):
        raise ValueError("results.json debe contener scenarios o cases")
    destination.mkdir(parents=True, exist_ok=False)

    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if any(part.startswith("data") for part in relative.parts) or path.is_symlink() or not path.is_file():
            continue
        if path.suffix not in (".log", ".inspect") and path.name not in ("results.json", "cluster.json", "wire-events.json")                 and not path.name.endswith(".committed.json"):
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)

    fields = ("case", "status", "coordinator", "commands", "commit", "abort", "pending", "duration_s", "election_s", "recovery_s", "initial_node",
              "initial_term", "successor_node", "successor_term", "sha256", "error")
    with (destination / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for scenario in scenarios:
            row = {key: scenario.get(key, "") for key in fields}
            for key in ("initial", "successor"):
                leader = scenario.get(key, {})
                row[f"{key}_node"] = leader.get("node", "")
                row[f"{key}_term"] = leader.get("term", "")
            row["case"] = scenario.get("case", scenario.get("name", ""))
            row["duration_s"] = scenario.get("duration_s", scenario.get("seconds", ""))
            row["sha256"] = json.dumps(scenario.get("sha256", scenario.get("hashes", {})), separators=(",", ":"))
            row["election_s"] = json.dumps(row["election_s"], separators=(",", ":")) if row["election_s"] else ""
            row["recovery_s"] = json.dumps(row["recovery_s"], separators=(",", ":")) if row["recovery_s"] else ""
            writer.writerow(row)
    print(f"Evidencias exportadas a {destination}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        export(args.source, args.destination)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
