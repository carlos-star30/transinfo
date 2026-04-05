param(
  [switch]$Backend,
  [switch]$Frontend,
  [switch]$All
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$stateFile = Join-Path $scriptDir ".local-service-state.json"

if (-not $Backend -and -not $Frontend -and -not $All) {
  $All = $true
}

function Stop-PythonProcessesByCommandLine {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Patterns,
    [Parameter(Mandatory = $true)]
    [string]$Label
  )

  function Test-CommandLineMatch {
    param(
      [string]$CommandLine,
      [string[]]$CandidatePatterns
    )

    if ([string]::IsNullOrWhiteSpace($CommandLine)) {
      return $false
    }

    $normalizedCommandLine = $CommandLine.ToLowerInvariant()
    foreach ($pattern in $CandidatePatterns) {
      if ([string]::IsNullOrWhiteSpace($pattern)) {
        continue
      }
      if ($normalizedCommandLine.Contains($pattern.ToLowerInvariant())) {
        return $true
      }
    }

    return $false
  }

  $processes = Get-CimInstance Win32_Process |
    Where-Object {
      $_.Name -eq "python.exe" -and
      (Test-CommandLineMatch -CommandLine $_.CommandLine -CandidatePatterns $Patterns)
    }

  if (-not $processes) {
    Write-Host "No $Label processes found." -ForegroundColor DarkGray
    return
  }

  foreach ($process in $processes) {
    Write-Host "Stopping $Label PID $($process.ProcessId)" -ForegroundColor Yellow
    try {
      taskkill /PID $process.ProcessId /T /F | Out-Null
    } catch {
      Write-Host "PID $($process.ProcessId) already exited." -ForegroundColor DarkGray
    }
  }
}

if ($All -or $Backend) {
  Stop-PythonProcessesByCommandLine -Patterns @("backend.import_status_api:app") -Label "backend"
}

if ($All -or $Frontend) {
  Stop-PythonProcessesByCommandLine -Patterns @("frontend-prototype/dev_proxy_server.py") -Label "frontend proxy"
}

$listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
  Where-Object { $_.LocalPort -in @(8000, 8001, 8002, 8088) } |
  Select-Object LocalAddress, LocalPort, OwningProcess, State

if ($listeners) {
  Write-Host "Remaining listeners:" -ForegroundColor Cyan
  $listeners | Format-Table -AutoSize
} else {
  Write-Host "No listeners remain on 8000/8001/8002/8088." -ForegroundColor Green
}

if (($All -or $Backend) -and (Test-Path $stateFile)) {
  Remove-Item $stateFile -Force
}