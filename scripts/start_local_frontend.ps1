$ErrorActionPreference = "Stop"

$env:DATAFLOW_DEV_HOST = "127.0.0.1"
$env:DATAFLOW_DEV_PORT = "8088"
$env:DATAFLOW_BACKEND_BASE = "http://127.0.0.1:8000"

Write-Host "Starting local frontend proxy..." -ForegroundColor Cyan
Write-Host "DATAFLOW_DEV_HOST=$env:DATAFLOW_DEV_HOST"
Write-Host "DATAFLOW_DEV_PORT=$env:DATAFLOW_DEV_PORT"
Write-Host "DATAFLOW_BACKEND_BASE=$env:DATAFLOW_BACKEND_BASE"

python frontend-prototype/dev_proxy_server.py