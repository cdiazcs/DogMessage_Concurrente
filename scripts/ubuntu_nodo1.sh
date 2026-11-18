#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build/java
javac -d build/java nodes/java/src/dogmessenger/NodeMain.java
java -cp build/java dogmessenger.NodeMain --id n1 --host 0.0.0.0 --port 9101
