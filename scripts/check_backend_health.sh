#!/usr/bin/env bash
set -euo pipefail

API_URL="${1:-http://127.0.0.1:8000}"
CHECK_URL="${API_URL}/api/import-status"

echo "[health] Checking ${CHECK_URL}"
status_code="$(curl -sS -o /tmp/check_backend_health.body -w '%{http_code}' "${CHECK_URL}")"

if [[ "${status_code}" == "200" || "${status_code}" == "401" || "${status_code}" == "403" ]]; then
	echo "[health] API reachable (HTTP ${status_code})"
	head -c 400 /tmp/check_backend_health.body && echo
else
	echo "[health] API check failed (HTTP ${status_code})" >&2
	head -c 400 /tmp/check_backend_health.body >&2 || true
	echo >&2
	exit 1
fi

echo "[health] Recent API logs:"
if command -v docker >/dev/null 2>&1; then
	docker compose logs --tail=60 api || true
else
	echo "[health] docker command not found, skipping container logs"
fi
