#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

BACKEND_HOST="${BACKEND_HOST:-0.0.0.0}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${FRONTEND_PORT:-8088}"
BACKEND_BASE="${DATAFLOW_BACKEND_BASE:-http://127.0.0.1:${BACKEND_PORT}}"
LOG_DIR="$ROOT_DIR/.artifacts/local-startup"
PID_DIR="$LOG_DIR/pids"

mkdir -p "$PID_DIR"

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

backend_log="$LOG_DIR/backend.log"
frontend_log="$LOG_DIR/frontend.log"

port_is_listening() {
	local port="$1"
	lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

wait_for_backend() {
	local attempts=60
	local url="http://127.0.0.1:${BACKEND_PORT}/api/import-status"
	local status

	for ((i=1; i<=attempts; i++)); do
		status="$(curl -sS -o /dev/null -w '%{http_code}' "$url" || true)"
		if [[ "$status" == "200" || "$status" == "401" || "$status" == "403" ]]; then
			return 0
		fi
		sleep 1
	done

	echo "Backend did not become ready within ${attempts}s. See $backend_log" >&2
	tail -n 40 "$backend_log" >&2 || true
	return 1
}

wait_for_frontend() {
	local attempts=30
	local url="http://127.0.0.1:${FRONTEND_PORT}/"
	local status

	for ((i=1; i<=attempts; i++)); do
		status="$(curl -I -sS -o /dev/null -w '%{http_code}' "$url" || true)"
		if [[ "$status" == "200" ]]; then
			return 0
		fi
		sleep 1
	done

	echo "Frontend did not become ready within ${attempts}s. See $frontend_log" >&2
	tail -n 40 "$frontend_log" >&2 || true
	return 1
}

parallels_host_ip() {
	if ifconfig bridge100 >/dev/null 2>&1; then
		ifconfig bridge100 | awk '/inet / {print $2; exit}'
	fi
}

echo "[start] Workspace: $ROOT_DIR"
echo "[start] Python: $PYTHON_BIN"

if port_is_listening "$BACKEND_PORT"; then
	echo "[start] Backend already listening on :$BACKEND_PORT"
else
	echo "[start] Starting backend on :$BACKEND_PORT"
	HOST="$BACKEND_HOST" PORT="$BACKEND_PORT" nohup "$ROOT_DIR/scripts/run_import_status_api.sh" >"$backend_log" 2>&1 &
	echo $! > "$PID_DIR/backend.pid"
	wait_for_backend
	backend_started=yes
fi

if port_is_listening "$FRONTEND_PORT"; then
	echo "[start] Frontend already listening on :$FRONTEND_PORT"
else
	echo "[start] Starting frontend on :$FRONTEND_PORT"
	DATAFLOW_DEV_HOST="$FRONTEND_HOST" DATAFLOW_DEV_PORT="$FRONTEND_PORT" DATAFLOW_BACKEND_BASE="$BACKEND_BASE" \
		nohup "$PYTHON_BIN" "$ROOT_DIR/frontend-prototype/dev_proxy_server.py" >"$frontend_log" 2>&1 &
	echo $! > "$PID_DIR/frontend.pid"
	wait_for_frontend
	frontend_started=yes
fi

if [[ "${backend_started:-no}" == "yes" ]]; then
	echo "[start] Backend ready"
fi

if [[ "${frontend_started:-no}" == "yes" ]]; then
	echo "[start] Frontend ready"
fi

echo
echo "Access URLs"
echo "- Mac browser: http://localhost:${FRONTEND_PORT}/"

parallels_ip="$(parallels_host_ip || true)"
if [[ -n "$parallels_ip" ]]; then
	echo "- Parallels Windows: http://${parallels_ip}:${FRONTEND_PORT}/"
fi

echo
echo "Logs"
echo "- Backend: $backend_log"
echo "- Frontend: $frontend_log"