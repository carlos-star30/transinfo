param(
	[string]$BindHost = "127.0.0.1",
	[int]$Port = 8088,
	[string]$BackendBase = "",
	[int]$BackendTimeoutSec = 1200,
	[switch]$KeepExisting
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$stateFile = Join-Path $scriptDir ".local-service-state.json"

if (-not $KeepExisting) {
	& (Join-Path $scriptDir "stop_local_services.ps1") -Frontend
}

if ([string]::IsNullOrWhiteSpace($BackendBase) -and (Test-Path $stateFile)) {
	$state = Get-Content $stateFile -Raw | ConvertFrom-Json
	if ($state.backendBase) {
		$BackendBase = [string]$state.backendBase
	}
}

if ([string]::IsNullOrWhiteSpace($BackendBase)) {
	$BackendBase = "http://127.0.0.1:8000"
}

$env:DATAFLOW_DEV_HOST = $BindHost
$env:DATAFLOW_DEV_PORT = [string]$Port
$env:DATAFLOW_BACKEND_BASE = $BackendBase.TrimEnd("/")
$env:DATAFLOW_BACKEND_TIMEOUT_SEC = [string]$BackendTimeoutSec

Write-Host "Starting local frontend proxy..." -ForegroundColor Cyan
Write-Host "DATAFLOW_DEV_HOST=$env:DATAFLOW_DEV_HOST"
Write-Host "DATAFLOW_DEV_PORT=$env:DATAFLOW_DEV_PORT"
Write-Host "DATAFLOW_BACKEND_BASE=$env:DATAFLOW_BACKEND_BASE"
Write-Host "DATAFLOW_BACKEND_TIMEOUT_SEC=$env:DATAFLOW_BACKEND_TIMEOUT_SEC"

python frontend-prototype/dev_proxy_server.py