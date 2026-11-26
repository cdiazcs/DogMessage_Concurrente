"""Ejecuta los vectores públicos en JCA, Kotlin/JVM y Web Crypto."""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    desktop = ROOT / "build/desktop/dog-messenger-desktop.jar"
    with tempfile.TemporaryDirectory(prefix="dog-e2ee-fixtures-") as temp:
        kotlin = Path(temp) / "e2ee-test.jar"
        subprocess.run(["javac", "--release", "17", "-cp", str(desktop), "-d", temp,
                        str(ROOT / "clients/desktop/test/E2eeVectors.java")], check=True, cwd=ROOT)
        subprocess.run(["java", "-cp", os.pathsep.join([temp, str(desktop)]), "E2eeVectors"], check=True, cwd=ROOT)
        mobile = ROOT / "clients/android/DogMessengerApp/app/src/main/java/com/pc4/dogmessenger/MobileE1.kt"
        subprocess.run(["kotlinc", str(ROOT / "clients/android/test/E2eeVectors.kt"), str(mobile),
                        "-classpath", str(desktop),
                        "-include-runtime", "-d", str(kotlin)], check=True, cwd=ROOT)
        subprocess.run(["java", "-cp", os.pathsep.join([str(kotlin), str(desktop)]), "E2eeVectorsKt"], check=True, cwd=ROOT)
    subprocess.run(["node", str(ROOT / "clients/web/e2ee_vectors.cjs")], check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
