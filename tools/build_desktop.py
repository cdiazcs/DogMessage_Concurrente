"""Compila y empaqueta el cliente Java Swing."""
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    sources = sorted((ROOT / "clients/desktop/src").glob("*.java"))
    output = ROOT / "build/desktop"
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as work:
        classes = Path(work) / "classes"
        classes.mkdir()
        jar = Path(work) / "dog-messenger-desktop.jar"
        subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-d", str(classes),
                        *map(str, sources)], check=True)
        subprocess.run(["jar", "--create", "--file", str(jar), "--main-class", "AppMain",
                        "-C", str(classes), "."], check=True)
        jar.replace(output / jar.name)
    print(f"Cliente Swing compilado: {output / jar.name}")


if __name__ == "__main__":
    main()
