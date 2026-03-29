#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo "[deploy] .env not found, creating from .env.example"
  cp .env.example .env
  echo "[deploy] Please edit .env and set DB_PASSWORD, then rerun."
  exit 1
fi

echo "[deploy] Building and starting backend services..."
docker compose up -d --build

echo "[deploy] Done. Current status:"
docker compose ps
