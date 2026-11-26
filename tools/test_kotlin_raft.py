"""Compila y ejecuta las pruebas de almacenamiento, elecciones y replicación Kotlin/JVM con el SDK estándar."""
from pathlib import Path
import shutil
import subprocess
import tempfile
from jvm_twopc import compile_module, bundle

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    kotlinc = shutil.which("kotlinc")
    java = shutil.which("java")
    if not kotlinc or not java:
        raise RuntimeError("Se requieren kotlinc y java en PATH")

    with tempfile.TemporaryDirectory(prefix="dog-kotlin-test-") as directory:
        artifact = Path(directory) / "raft-tests.jar"
        sources = sorted((ROOT / "nodes/kotlin/src").rglob("*.kt"))
        sources += sorted((ROOT / "nodes/kotlin/test").rglob("*.kt"))
        classes = str(Path(directory) / "classes")
        compile_module(classes)
        subprocess.run([kotlinc, *map(str, sources), "-classpath", classes, "-include-runtime", "-d", str(artifact)], check=True)
        bundle(artifact, classes)
        for suite in ("RaftStorageTest", "RaftElectionTest", "RaftReplicationTest", "TwoPcWireTest", "TwoPcEngineTest", "ChatEnvelopeTest"):
            subprocess.run([java, "-cp", str(artifact), suite], check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
