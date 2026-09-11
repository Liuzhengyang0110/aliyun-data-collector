[CmdletBinding()]
param(
    [switch]$Docker,
    [int]$MockCcPort = 9443,
    [int]$MockArmsPort = 9410,
    [string]$Password = 'testbed-changeit'
)

$ErrorActionPreference = 'Stop'
$testbed = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$pki = Join-Path $testbed '.generated\pki'
$run = Join-Path $testbed '.generated\run'
$state = Join-Path $testbed '.state\mock-cc-ingest'
$jar = Join-Path $testbed 'mock-cc-ingest\target\mock-cc-ingest-0.1.0-SNAPSHOT-all.jar'
$armsJar = Join-Path $testbed 'mock-arms\target\mock-arms-0.1.0-SNAPSHOT-all.jar'
$java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\java.exe' } else { (Get-Command java -ErrorAction Stop).Source }

if (-not (Test-Path -LiteralPath (Join-Path $pki 'server.p12'))) {
    & (Join-Path $PSScriptRoot 'generate-test-pki.ps1') -Password $Password
}
if (-not (Test-Path -LiteralPath $jar) -or -not (Test-Path -LiteralPath $armsJar)) {
    & mvn -f (Join-Path $testbed 'pom.xml') package
    if ($LASTEXITCODE -ne 0) { throw 'testbed build failed' }
}

if ($Docker) {
    $env:TESTBED_KEYSTORE_PASSWORD = $Password
    & docker compose -f (Join-Path $testbed 'docker-compose.yml') up -d --build
    if ($LASTEXITCODE -ne 0) { throw 'docker compose failed' }
    return
}

New-Item -ItemType Directory -Path $run -Force | Out-Null
New-Item -ItemType Directory -Path $state -Force | Out-Null
$pidFile = Join-Path $run 'mock-cc-ingest.json'
$armsPidFile = Join-Path $run 'mock-arms.json'
if ((Test-Path -LiteralPath $pidFile) -or (Test-Path -LiteralPath $armsPidFile)) { throw 'testbed PID file already exists; run stop-testbed.ps1 first.' }

$env:MOCK_ARMS_EXPECTED_ACCESS_KEY_ID = 'testbed-access-key-id'
$env:MOCK_ARMS_DATASET_USERNAME = 'testbed-user'
$env:MOCK_ARMS_DATASET_PASSWORD = 'testbed-password'
$armsProcess = Start-Process -FilePath $java -ArgumentList @('-jar', $armsJar, '--bind', '127.0.0.1', '--port', $MockArmsPort) `
    -WorkingDirectory $testbed -WindowStyle Hidden -RedirectStandardOutput (Join-Path $run 'mock-arms.stdout.log') `
    -RedirectStandardError (Join-Path $run 'mock-arms.stderr.log') -PassThru
@{ pid = $armsProcess.Id; started_at = $armsProcess.StartTime.ToUniversalTime().ToString('O') } |
    ConvertTo-Json | Set-Content -LiteralPath $armsPidFile -Encoding utf8NoBOM

$env:MOCK_CC_KEYSTORE_PASSWORD = $Password
$env:MOCK_CC_TRUSTSTORE_PASSWORD = $Password
$arguments = @('-jar', $jar,
    '--bind', '127.0.0.1', '--port', $MockCcPort,
    '--state-dir', $state,
    '--key-store', (Join-Path $pki 'server.p12'),
    '--trust-store', (Join-Path $pki 'server-trust.p12'),
    '--collector-cert-map', (Join-Path $pki 'collector-cert-map.json'))
$process = Start-Process -FilePath $java -ArgumentList $arguments -WorkingDirectory $testbed `
    -WindowStyle Hidden -RedirectStandardOutput (Join-Path $run 'mock-cc-ingest.stdout.log') `
    -RedirectStandardError (Join-Path $run 'mock-cc-ingest.stderr.log') -PassThru
@{ pid = $process.Id; started_at = $process.StartTime.ToUniversalTime().ToString('O') } |
    ConvertTo-Json | Set-Content -LiteralPath $pidFile -Encoding utf8NoBOM

$deadline = [DateTime]::UtcNow.AddSeconds(20)
do {
    if ($process.HasExited) { throw "mock-cc-ingest exited early with code $($process.ExitCode)" }
    if ($armsProcess.HasExited) { throw "mock-arms exited early with code $($armsProcess.ExitCode)" }
    try {
        $ccTcp = [System.Net.Sockets.TcpClient]::new()
        $ccTcp.Connect('127.0.0.1', $MockCcPort)
        $ccTcp.Dispose()
        $armsTcp = [System.Net.Sockets.TcpClient]::new()
        $armsTcp.Connect('127.0.0.1', $MockArmsPort)
        $armsTcp.Dispose()
        Write-Host "mock-cc-ingest is listening on https://127.0.0.1:$MockCcPort"
        Write-Host "mock-arms is listening on http://127.0.0.1:$MockArmsPort"
        return
    } catch {
        Start-Sleep -Milliseconds 200
    }
} while ([DateTime]::UtcNow -lt $deadline)
throw 'testbed services did not both start within 20 seconds'
