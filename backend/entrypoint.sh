#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] Waiting for MySQL..."
python - <<'PY'
import os
import time
from pathlib import Path
import mysql.connector

host = os.getenv("DB_HOST", "db")
port = int(os.getenv("DB_PORT", "3306"))
user = os.getenv("DB_USER", "root")
password = os.getenv("DB_PASSWORD", "showlang")
ssl_ca = os.getenv("DB_SSL_CA", "").strip()
ssl_disabled = os.getenv("DB_SSL_DISABLED", "false").strip().lower() == "true"
ssl_verify_cert = os.getenv("DB_SSL_VERIFY_CERT", "false").strip().lower() == "true"
ssl_verify_identity = os.getenv("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"
ssl_ca_path = Path(ssl_ca) if ssl_ca else None

connect_kwargs = {
    "host": host,
    "port": port,
    "user": user,
    "password": password,
}

if ssl_disabled:
    connect_kwargs["ssl_disabled"] = True
elif ssl_ca_path and ssl_ca_path.exists():
    connect_kwargs["ssl_ca"] = ssl_ca
    connect_kwargs["ssl_verify_cert"] = ssl_verify_cert
    connect_kwargs["ssl_verify_identity"] = ssl_verify_identity
elif ssl_ca:
    print(f"[entrypoint] DB_SSL_CA not found, continuing without explicit CA bundle: {ssl_ca}")

deadline = time.time() + 120
while True:
    try:
        conn = mysql.connector.connect(**connect_kwargs)
        conn.close()
        print("[entrypoint] MySQL is ready")
        break
    except Exception as exc:
        if time.time() > deadline:
            raise SystemExit(f"MySQL not ready after 120s: {exc}")
        time.sleep(2)
PY

echo "[entrypoint] Ensuring tables..."
python /app/scripts/create_rstran_table.py
python /app/scripts/create_bw_object_name_table.py
python /app/scripts/create_dd03l_table.py

APP_PORT="${PORT:-8000}"

echo "[entrypoint] Starting API on :${APP_PORT}"
exec uvicorn backend.import_status_api:app --host 0.0.0.0 --port "$APP_PORT"
