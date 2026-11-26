"""Compila e incorpora los módulos JVM compartidos a un artefacto Kotlin."""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def compile_module(output):
    sources = sorted(path for package in ("twopc", "chat", "media")
                     for path in (ROOT / "nodes/java/src/dogmessenger" / package).glob("*.java"))
    subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-d", str(output), *map(str, sources)], check=True)


def bundle(artifact, classes):
    subprocess.run(["jar", "--update", "--file", str(artifact), "-C", str(classes), "dogmessenger/twopc", "-C", str(classes), "dogmessenger/chat", "-C", str(classes), "dogmessenger/media"], check=True)
