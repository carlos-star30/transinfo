$ErrorActionPreference = "Stop"

$env:DB_DRIVER = "sqlite"
$env:SQLITE_DB_PATH = "backend/data/trans_fields_mapping.db"

Write-Host "Starting local backend with SQLite..." -ForegroundColor Cyan
Write-Host "DB_DRIVER=$env:DB_DRIVER"
Write-Host "SQLITE_DB_PATH=$env:SQLITE_DB_PATH"

python -m uvicorn backend.import_status_api:app --host 127.0.0.1 --port 8000 --reload