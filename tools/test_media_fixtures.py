"""Verifica fixtures DMB1 con el módulo distribuidor de Java y el empaquetado Kotlin."""
from pathlib import Path
import os
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    java = ROOT / "build/java"
    kotlin = ROOT / "build/kotlin/dog-messenger-kotlin.jar"
    suite = "dogmessenger.MediaWireTest"
    source = ROOT / "nodes/java/test/dogmessenger/MediaWireTest.java"
    with tempfile.TemporaryDirectory() as temporary:
        subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-cp", str(java),
                        "-d", temporary, str(source)], check=True)
        for artifact in (java, kotlin):
            print(artifact.relative_to(ROOT), flush=True)
            subprocess.run(["java", "-cp", os.pathsep.join((temporary, str(artifact))), suite],
                           cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
