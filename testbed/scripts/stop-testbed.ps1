[CmdletBinding()]
param([switch]$Docker)

$ErrorActionPreference = 'Stop'
$testbed = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ($Docker) {
    & docker compose -f (Join-Path $testbed 'docker-compose.yml') down
    if ($LASTEXITCODE -ne 0) { throw 'docker compose down failed' }
    return
}

foreach ($service in @('mock-cc-ingest', 'mock-arms')) {
    $pidFile = Join-Path $testbed ".generated\run\$service.json"
    if (-not (Test-Path -LiteralPath $pidFile)) {
        Write-Host "No $service PID file found."
        continue
    }
    $record = Get-Content -LiteralPath $pidFile -Raw | ConvertFrom-Json -DateKind String
    $process = Get-Process -Id ([int]$record.pid) -ErrorAction SilentlyContinue
    if ($null -ne $process) {
        $recordedStart = [DateTimeOffset]::Parse([string]$record.started_at,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind).UtcDateTime
        $actualStart = $process.StartTime.ToUniversalTime()
        if ($process.ProcessName -notlike 'java*' -or [Math]::Abs(($actualStart - $recordedStart).TotalSeconds) -gt 2) {
            throw "PID for $service was reused; refusing to stop it."
        }
        Stop-Process -Id $process.Id
        $process.WaitForExit(10000) | Out-Null
    }
    Remove-Item -LiteralPath $pidFile -Force
    Write-Host "$service stopped."
}
