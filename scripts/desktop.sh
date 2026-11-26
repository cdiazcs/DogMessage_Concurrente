#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ ${1:-} == --build ]]; then
    python3 tools/build_desktop.py
    shift
fi
exec java "$@" -jar build/desktop/dog-messenger-desktop.jar
