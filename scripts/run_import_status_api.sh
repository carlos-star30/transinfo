#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
	PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
	PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
	PYTHON_BIN="$(command -v python)"
else
	echo "No Python interpreter found. Create .venv or install python3." >&2
	exit 1
fi

"$PYTHON_BIN" -m uvicorn backend.import_status_api:app --host "$HOST" --port "$PORT" --reload
