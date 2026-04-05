param(
	[string]$BindHost = "127.0.0.1",
	[int]$Port = 8000,
	[string]$SqliteDbPath = "backend/data/trans_fields_mapping.db",
	[switch]$Reload,
	[switch]$KeepExisting
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$stateFile = Join-Path $scriptDir ".local-service-state.json"

if (-not $KeepExisting) {
	& (Join-Path $scriptDir "stop_local_services.ps1") -Backend
}

function Test-PortListening {
	param([int]$CandidatePort)
	return [bool](Get-NetTCPConnection -LocalPort $CandidatePort -State Listen -ErrorAction SilentlyContinue)
}

function Resolve-BackendPort {
	param([int]$PreferredPort)

	$candidatePorts = @($PreferredPort, 8002, 8001, 8010) | Select-Object -Unique
	foreach ($candidate in $candidatePorts) {
		if (-not (Test-PortListening -CandidatePort $candidate)) {
			return $candidate
		}
	}

	throw "No available backend port found in candidates: $($candidatePorts -join ', ')"
}

$resolvedPort = Resolve-BackendPort -PreferredPort $Port

$env:DB_DRIVER = "sqlite"
$env:SQLITE_DB_PATH = $SqliteDbPath

Write-Host "Starting local backend with SQLite..." -ForegroundColor Cyan
Write-Host "DB_DRIVER=$env:DB_DRIVER"
Write-Host "SQLITE_DB_PATH=$env:SQLITE_DB_PATH"
Write-Host "HOST=$BindHost"
Write-Host "PORT=$resolvedPort"
Write-Host "RELOAD=$([bool]$Reload)"

@{
	backendBase = "http://127.0.0.1:$resolvedPort"
	backendPort = $resolvedPort
	updatedAt = (Get-Date).ToString("s")
} | ConvertTo-Json | Set-Content -Path $stateFile -Encoding UTF8

$uvicornArgs = @("-m", "uvicorn", "backend.import_status_api:app", "--host", $BindHost, "--port", [string]$resolvedPort)
if ($Reload) {
	$uvicornArgs += "--reload"
}

python @uvicornArgs