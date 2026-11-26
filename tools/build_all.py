"""Compila Java y Kotlin usando únicamente sus compiladores oficiales."""
from __future__ import annotations
import argparse
import shutil
import subprocess
import tempfile
from jvm_twopc import compile_module, bundle
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_java():
    javac = shutil.which("javac")
    if not javac:
        raise RuntimeError("Falta javac (JDK)")
    out = ROOT / "build" / "java"
    out.mkdir(parents=True, exist_ok=True)
    sources = sorted((ROOT / "nodes/java/src").rglob("*.java"))
    subprocess.run([javac, "--release", "17", "-encoding", "UTF-8", "-d", str(out), *map(str, sources)], check=True)
    print("OK Java compilado")


def build_kotlin():
    kotlinc = shutil.which("kotlinc")
    if not kotlinc:
        raise RuntimeError("Falta kotlinc (compilador Kotlin)")
    out = ROOT / "build" / "kotlin"
    out.mkdir(parents=True, exist_ok=True)
    sources = [str(path) for path in sorted((ROOT / "nodes/kotlin/src").rglob("*.kt"))]
    artifact = out / "dog-messenger-kotlin.jar"
    with tempfile.TemporaryDirectory(prefix="dog-kotlin-", dir=out) as temporary:
        staged = Path(temporary) / artifact.name
        classes = Path(temporary) / "classes"
        compile_module(classes)
        subprocess.run([kotlinc, *sources, "-classpath", str(classes), "-include-runtime", "-d", str(staged)], check=True)
        bundle(staged, classes)
        staged.replace(artifact)
    print("OK Kotlin compilado")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--desktop", action="store_true", help="Incluye el cliente Java Swing")
    parser.add_argument("--java-only", action="store_true")
    parser.add_argument("--kotlin-only", action="store_true")
    args = parser.parse_args()
    if args.java_only and args.kotlin_only:
        parser.error("Selecciona solo una opción")
    if not args.kotlin_only:
        build_java()
    if not args.java_only:
        build_kotlin()
    if args.desktop:
        from build_desktop import main as build_desktop
        build_desktop()
