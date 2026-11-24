"""Benchmark comparativo (Raft vs 2PC) y generador automático de reportes y gráficos."""
from __future__ import annotations

import csv
import shutil
import socket
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
NODE_SCRIPT = ROOT / "nodes" / "python" / "node.py"
REPORTS_DIR = ROOT / "reports"
sys.path.insert(0, str(ROOT / "nodes" / "python"))

from node import encode_frame, recv_frame
from protocol_dm2 import build_dm2, parse_dm2

CLUSTER = {
    "n1": ("127.0.0.1", 9301),
    "n2": ("127.0.0.1", 9302),
    "n3": ("127.0.0.1", 9303),
}


def send_cmd(addr: Tuple[str, int], msg_type: str, payload: Dict[str, Any], timeout: float = 3.0) -> Dict[str, Any]:
    with socket.create_connection(addr, timeout=timeout) as conn:
        conn.settimeout(timeout)
        conn.sendall(encode_frame(build_dm2(msg_type, "bench-client", payload)))
        return parse_dm2(recv_frame(conn))


def wait_leader(timeout: float = 5.0) -> Tuple[str, Tuple[str, int]]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for nid, addr in CLUSTER.items():
            try:
                resp = send_cmd(addr, "NODE_STATUS", {}, timeout=0.4)
                if resp["payload"]["metrics"]["state"] == "LEADER":
                    return nid, addr
            except OSError:
                pass
        time.sleep(0.1)
    return "n1", CLUSTER["n1"]


def benchmark_algorithm(algo: str, num_messages: int = 100) -> Dict[str, Any]:
    data_dir = ROOT / f"data_bench_{algo}"
    if data_dir.exists():
        shutil.rmtree(data_dir)

    peers_arg = ",".join(f"{nid}={h}:{p}" for nid, (h, p) in CLUSTER.items())
    procs: List[subprocess.Popen] = []

    try:
        for nid, (host, port) in CLUSTER.items():
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(NODE_SCRIPT),
                        "--id",
                        nid,
                        "--host",
                        host,
                        "--port",
                        str(port),
                        "--peers",
                        peers_arg,
                        "--algo",
                        algo,
                        "--data-dir",
                        str(data_dir),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )

        time.sleep(1.0)
        leader_id, leader_addr = wait_leader(4.0)
        latencies_ms: List[float] = []
        errors = 0

        t_total_start = time.perf_counter()
        for i in range(num_messages):
            req_id = uuid.uuid4().hex
            t0 = time.perf_counter()
            try:
                resp = send_cmd(
                    leader_addr,
                    "CLIENT_COMMAND",
                    {"command": f"bench-{algo}-{i}", "requestId": req_id},
                    timeout=3.0,
                )
                dt_ms = (time.perf_counter() - t0) * 1000.0
                if resp["payload"].get("committed"):
                    latencies_ms.append(dt_ms)
                else:
                    errors += 1
                    _, leader_addr = wait_leader(2.0)
            except OSError:
                errors += 1
                _, leader_addr = wait_leader(2.0)

        total_sec = time.perf_counter() - t_total_start
        committed = len(latencies_ms)
        throughput = committed / total_sec if total_sec > 0 else 0.0

        lat_sorted = sorted(latencies_ms) if latencies_ms else [0.0]
        p50 = statistics.median(lat_sorted)
        p95 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.95))]
        p99 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.99))]
        mean_ms = statistics.mean(lat_sorted)

        # Recolectar métricas internas de RPCs
        total_rpc_sent = 0
        for addr in CLUSTER.values():
            try:
                st = send_cmd(addr, "NODE_STATUS", {}, timeout=0.5)
                total_rpc_sent += st["payload"]["metrics"].get("rpc_sent", 0)
            except OSError:
                pass

        return {
            "algorithm": algo.upper(),
            "messages_sent": num_messages,
            "committed": committed,
            "errors": errors,
            "total_time_s": round(total_sec, 4),
            "throughput_ops_s": round(throughput, 2),
            "latency_mean_ms": round(mean_ms, 3),
            "latency_p50_ms": round(p50, 3),
            "latency_p95_ms": round(p95, 3),
            "latency_p99_ms": round(p99, 3),
            "total_rpc_sent": total_rpc_sent,
            "latencies_raw": latencies_ms,
        }
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
        if data_dir.exists():
            shutil.rmtree(data_dir, ignore_errors=True)


def generate_outputs(results: List[Dict[str, Any]]) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Exportar CSV
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
        "total_rpc_sent",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    # 2. Exportar Reporte Markdown
    md_path = REPORTS_DIR / "INFORME_RENDIMIENTO.md"
    lines = [
        "# Reporte Automatizado de Rendimiento — Dog Messenger (PC4)",
        "",
        "## Comparativa de Algoritmos de Consenso",
        "",
        "| Algoritmo | Confirmados | Errores | Throughput (msg/s) | Latencia Media (ms) | p50 (ms) | p95 (ms) | p99 (ms) | RPCs de Red |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in results:
        lines.append(
            f"| **{r['algorithm']}** | {r['committed']}/{r['messages_sent']} | {r['errors']} | "
            f"{r['throughput_ops_s']} | {r['latency_mean_ms']} | {r['latency_p50_ms']} | "
            f"{r['latency_p95_ms']} | {r['latency_p99_ms']} | {r['total_rpc_sent']} |"
        )
    lines.extend(
        [
            "",
            "## Observaciones Técnicas",
            "- **Raft** compromete entradas al alcanzar mayoría (2 de 3 nodos), tolerando la caída de 1 nodo sin detener el servicio.",
            "- **2PC (Two-Phase Commit)** requiere unanimidad de participantes en la fase `PREPARE`, bloqueando el compromiso si un nodo cae durante la transacción.",
        ]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")

    # 3. Generar Gráficos PNG si matplotlib está disponible
    try:
        import matplotlib.pyplot as plt

        algos = [r["algorithm"] for r in results]
        throughputs = [r["throughput_ops_s"] for r in results]
        p50s = [r["latency_p50_ms"] for r in results]
        p95s = [r["latency_p95_ms"] for r in results]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

        # Gráfico 1: Throughput
        ax1.bar(algos, throughputs, color=["#2b6cb0", "#2f855a"])
        ax1.set_title("Throughput de Consenso (mensajes/s)")
        ax1.set_ylabel("Mensajes comprometidos / seg")
        ax1.grid(axis="y", linestyle="--", alpha=0.5)

        # Gráfico 2: Latencia p50 vs p95
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
        print("[AVISO] matplotlib no instalado; instala con 'pip install matplotlib' para generar gráficos PNG.")

    print(f"[OK] CSV generado en: {csv_path}")
    print(f"[OK] Reporte Markdown generado en: {md_path}")


def main() -> int:
    print("Ejecutando benchmark comparativo (100 mensajes por algoritmo)...")
    res_raft = benchmark_algorithm("raft", num_messages=100)
    print(f" -> Raft: {res_raft['throughput_ops_s']} msg/s | p50={res_raft['latency_p50_ms']} ms")
    res_2pc = benchmark_algorithm("2pc", num_messages=100)
    print(f" -> 2PC:  {res_2pc['throughput_ops_s']} msg/s | p50={res_2pc['latency_p50_ms']} ms")

    generate_outputs([res_raft, res_2pc])
    return 0


if __name__ == "__main__":
    sys.exit(main())