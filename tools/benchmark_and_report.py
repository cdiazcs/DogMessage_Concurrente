"""Benchmark comparativo (Raft vs 2PC) y generador automático de reportes y gráficos sobre DM2 binario."""
from __future__ import annotations

import csv
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cluster_lib import Cluster  # noqa: E402

PORTS_RAFT = {"n1": 9301, "n2": 9302, "n3": 9303}


def benchmark_raft(num_messages: int = 100) -> Dict[str, Any]:
    workdir = Path(tempfile.mkdtemp(prefix="dm-bench-raft-"))
    cluster = Cluster([(nid, "python", port) for nid, port in PORTS_RAFT.items()], workdir)
    latencies_ms: List[float] = []
    errors = 0

    try:
        for node in cluster.nodes.values():
            node.start()
        leader = cluster.find_leader(10)
        if leader is None:
            raise RuntimeError("No se eligió líder Raft para el benchmark")

        t_total_start = time.perf_counter()
        for i in range(num_messages):
            t0 = time.perf_counter()
            try:
                cluster.send_command(f"bench-raft-{i}".encode())
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            except Exception:
                errors += 1

        total_sec = time.perf_counter() - t_total_start
        committed = len(latencies_ms)
        throughput = committed / total_sec if total_sec > 0 else 0.0

        lat_sorted = sorted(latencies_ms) if latencies_ms else [0.0]
        p50 = statistics.median(lat_sorted)
        p95 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.95))]
        p99 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.99))]
        mean_ms = statistics.mean(lat_sorted)

        return {
            "algorithm": "RAFT",
            "messages_sent": num_messages,
            "committed": committed,
            "errors": errors,
            "total_time_s": round(total_sec, 4),
            "throughput_ops_s": round(throughput, 2),
            "latency_mean_ms": round(mean_ms, 3),
            "latency_p50_ms": round(p50, 3),
            "latency_p95_ms": round(p95, 3),
            "latency_p99_ms": round(p99, 3),
        }
    finally:
        cluster.stop_all()
        shutil.rmtree(workdir, ignore_errors=True)


def benchmark_2pc(num_messages: int = 100) -> Dict[str, Any]:
    """Ejecuta el benchmark del segundo algoritmo usando la herramienta oficial de 2PC del repo."""
    cli_2pc = ROOT / "tools" / "two_pc_cli.py"
    node_py = ROOT / "nodes" / "python" / "node.py"
    workdir = Path(tempfile.mkdtemp(prefix="dm-bench-2pc-"))
    ports = {"n1": 9401, "n2": 9402, "n3": 9403}
    peers_arg = ",".join(f"{nid}=127.0.0.1:{p}" for nid, p in ports.items())
    procs: List[subprocess.Popen] = []
    latencies_ms: List[float] = []
    errors = 0

    try:
        for nid, port in ports.items():
            ndir = workdir / nid
            ndir.mkdir(parents=True, exist_ok=True)
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(node_py),
                        "--id",
                        nid,
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--peers",
                        peers_arg,
                        "--algo",
                        "2pc",
                        "--data-dir",
                        str(ndir),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
        time.sleep(0.8)

        t_total_start = time.perf_counter()
        for i in range(num_messages):
            t0 = time.perf_counter()
            res = subprocess.run(
                [
                    sys.executable,
                    str(cli_2pc),
                    "--coordinator",
                    f"127.0.0.1:{ports['n1']}",
                    "--participants",
                    f"n1=127.0.0.1:{ports['n1']},n2=127.0.0.1:{ports['n2']},n3=127.0.0.1:{ports['n3']}",
                    "--command",
                    f"bench-2pc-{i}",
                ],
                capture_output=True,
                text=True,
                timeout=3,
            )
            if res.returncode == 0:
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            else:
                errors += 1

        total_sec = time.perf_counter() - t_total_start
        committed = len(latencies_ms)
        throughput = committed / total_sec if total_sec > 0 else 0.0

        lat_sorted = sorted(latencies_ms) if latencies_ms else [0.0]
        p50 = statistics.median(lat_sorted)
        p95 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.95))]
        p99 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.99))]
        mean_ms = statistics.mean(lat_sorted)

        return {
            "algorithm": "2PC",
            "messages_sent": num_messages,
            "committed": committed,
            "errors": errors,
            "total_time_s": round(total_sec, 4),
            "throughput_ops_s": round(throughput, 2),
            "latency_mean_ms": round(mean_ms, 3),
            "latency_p50_ms": round(p50, 3),
            "latency_p95_ms": round(p95, 3),
            "latency_p99_ms": round(p99, 3),
        }
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
        shutil.rmtree(workdir, ignore_errors=True)


def generate_outputs(results: List[Dict[str, Any]]) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = REPORTS_DIR / "resultados_consenso.csv"
    fields = [
        "algorithm",
        "messages_sent",
        "committed",
        "errors",
        "total_time_s",
        "throughput_ops_s",
        "latency_mean_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    md_path = REPORTS_DIR / "INFORME_RENDIMIENTO.md"
    lines = [
        "# Reporte Automatizado de Rendimiento — Dog Messenger (PC4)",
        "",
        "## Comparativa de Algoritmos de Consenso (Protocolo DM2 Binario)",
        "",
        "| Algoritmo | Confirmados | Errores | Throughput (msg/s) | Latencia Media (ms) | p50 (ms) | p95 (ms) | p99 (ms) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in results:
        lines.append(
            f"| **{r['algorithm']}** | {r['committed']}/{r['messages_sent']} | {r['errors']} | "
            f"{r['throughput_ops_s']} | {r['latency_mean_ms']} | {r['latency_p50_ms']} | "
            f"{r['latency_p95_ms']} | {r['latency_p99_ms']} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")

    try:
        import matplotlib.pyplot as plt

        algos = [r["algorithm"] for r in results]
        throughputs = [r["throughput_ops_s"] for r in results]
        p50s = [r["latency_p50_ms"] for r in results]
        p95s = [r["latency_p95_ms"] for r in results]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

        ax1.bar(algos, throughputs, color=["#2b6cb0", "#2f855a"])
        ax1.set_title("Throughput de Consenso (mensajes/s)")
        ax1.set_ylabel("Mensajes comprometidos / seg")
        ax1.grid(axis="y", linestyle="--", alpha=0.5)

        x = range(len(algos))
        width = 0.35
        ax2.bar([i - width / 2 for i in x], p50s, width, label="p50 (Mediana)", color="#4299e1")
        ax2.bar([i + width / 2 for i in x], p95s, width, label="p95", color="#ed8936")
        ax2.set_xticks(list(x))
        ax2.set_xticklabels(algos)
        ax2.set_title("Latencia hasta Compromiso (ms)")
        ax2.set_ylabel("Milisegundos (ms)")
        ax2.legend()
        ax2.grid(axis="y", linestyle="--", alpha=0.5)

        plt.tight_layout()
        chart_path = REPORTS_DIR / "comparativa_consenso.png"
        plt.savefig(chart_path, dpi=150)
        plt.close(fig)
        print(f"[OK] Gráfico generado en: {chart_path}")
    except ImportError:
        print("[AVISO] matplotlib no instalado.")

    print(f"[OK] CSV generado en: {csv_path}")
    print(f"[OK] Reporte Markdown generado en: {md_path}")


def main() -> int:
    print("Ejecutando benchmark comparativo sobre DM2 binario (100 mensajes por algoritmo)...")
    res_raft = benchmark_raft(num_messages=100)
    print(f" -> Raft: {res_raft['throughput_ops_s']} msg/s | p50={res_raft['latency_p50_ms']} ms")
    res_2pc = benchmark_2pc(num_messages=100)
    print(f" -> 2PC:  {res_2pc['throughput_ops_s']} msg/s | p50={res_2pc['latency_p50_ms']} ms")

    generate_outputs([res_raft, res_2pc])
    return 0


if __name__ == "__main__":
    sys.exit(main())