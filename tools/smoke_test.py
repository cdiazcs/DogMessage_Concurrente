"""Prueba real local: inicia tres nodos y verifica seis PING cruzados."""
from __future__ import annotations
import argparse
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from build_all import ROOT, build_java, build_kotlin


def is_listening(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.15):
            return True
    except OSError:
        return False


def wait_for(port: int, process: subprocess.Popen, limit=12):
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"El nodo del puerto {port} terminó antes de iniciar (exit={process.returncode})")
        if is_listening(port):
            return
        time.sleep(0.1)
    raise RuntimeError(f"No inició nodo {port}; comprueba que el puerto esté libre")


def run(skip_kotlin: bool):
    build_java()
    if not skip_kotlin:
        build_kotlin()
    py = [sys.executable, str(ROOT / "nodes/python/node.py")]
    java = ["java", "-cp", str(ROOT / "build/java"), "dogmessenger.NodeMain"]
    kotlin = ["java", "-jar", str(ROOT / "build/kotlin/dog-messenger-kotlin.jar")]
    selected = [("n1", java, 19101), ("n2", py, 19102)]
    if not skip_kotlin:
        selected.append(("n3", kotlin, 19103))
    processes = []
    with tempfile.TemporaryFile(mode="w+t") as logs:
        try:
            for node_id, cmd, port in selected:
                if is_listening(port):
                    raise RuntimeError(f"Puerto {port} ocupado; cierra otras instancias")
                proc = subprocess.Popen(cmd + ["--id", node_id, "--host", "127.0.0.1", "--port", str(port)], stdout=logs, stderr=subprocess.STDOUT)
                processes.append(proc)
                wait_for(port, proc)
            checks = 0
            for origin, cmd, _ in selected:
                for destination, _, dest_port in selected:
                    if origin == destination:
                        continue
                    test = subprocess.run(cmd + ["--id", origin, "--ping", f"127.0.0.1:{dest_port}"], capture_output=True, text=True, timeout=12)
                    if test.returncode != 0 or f"PONG de {destination}" not in test.stdout:
                        raise RuntimeError(f"Prueba {origin}->{destination} falló:\n{test.stdout}\n{test.stderr}")
                    print(f"OK {origin} -> {destination}")
                    checks += 1
            print(f"\nPASS: {checks}/{checks} PING TCP interoperables")
        except Exception:
            logs.seek(0)
            print("\n--- Logs de servidores ---\n" + logs.read(), file=sys.stderr)
            raise
        finally:
            for proc in processes:
                proc.terminate()
            for proc in processes:
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-kotlin", action="store_true", help="Solo diagnóstico Python/Java; no completa prueba trilingüe")
    args = ap.parse_args()
    run(args.skip_kotlin)
