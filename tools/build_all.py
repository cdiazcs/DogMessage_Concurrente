"""Compila Java y Kotlin usando únicamente sus compiladores oficiales."""
from __future__ import annotations
import argparse
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_java():
    if not shutil.which("javac"):
        raise RuntimeError("Falta javac (JDK)")
    out = ROOT / "build" / "java"
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run(["javac", "-d", str(out), str(ROOT / "nodes/java/src/dogmessenger/NodeMain.java")], check=True)
    print("OK Java compilado")


def build_kotlin():
    if not shutil.which("kotlinc"):
        raise RuntimeError("Falta kotlinc (compilador Kotlin)")
    out = ROOT / "build" / "kotlin"
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run(["kotlinc", str(ROOT / "nodes/kotlin/src/NodeMain.kt"), "-include-runtime", "-d", str(out / "dog-messenger-kotlin.jar")], check=True)
    print("OK Kotlin compilado")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--java-only", action="store_true")
    parser.add_argument("--kotlin-only", action="store_true")
    args = parser.parse_args()
    if args.java_only and args.kotlin_only:
        parser.error("Selecciona solo una opción")
    if not args.kotlin_only:
        build_java()
    if not args.java_only:
        build_kotlin()
