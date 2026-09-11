[CmdletBinding()]
param(
    [int]$MockCcPort = 9443,
    [int]$MockArmsPort = 9410,
    [string]$Password = 'testbed-changeit',
    [int]$TimeoutSeconds = 90
)

$ErrorActionPreference = 'Stop'
$testbed = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$project = [System.IO.Path]::GetFullPath((Join-Path $testbed '..'))
$pki = Join-Path $testbed '.generated\pki'
$smokeRoot = Join-Path $testbed '.generated\smoke'
$dataRoot = Join-Path $smokeRoot 'collector-data'
$runRoot = Join-Path $testbed '.generated\run'
$mockCcJar = Join-Path $testbed 'mock-cc-ingest\target\mock-cc-ingest-0.1.0-SNAPSHOT-all.jar'
$mockArmsJar = Join-Path $testbed 'mock-arms\target\mock-arms-0.1.0-SNAPSHOT-all.jar'
$collectorJar = Join-Path $project 'target\aliyun-data-collector-0.1.0-SNAPSHOT.jar'
$sourceConfig = Join-Path $testbed 'config\collector-mock.example.yaml'
$runtimeConfig = Join-Path $smokeRoot 'collector-smoke.yaml'
$java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\java.exe' } else { (Get-Command java -ErrorAction Stop).Source }
$collectorProcess = $null
$ccProcess = $null
$armsProcess = $null

function Test-TcpPort {
    param([int]$Port)
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $client.Connect('127.0.0.1', $Port)
        $client.Dispose()
        return $true
    } catch { return $false }
}

function Wait-TcpPort {
    param([int]$Port, [System.Diagnostics.Process]$Process, [string]$Name)
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        if ($Process.HasExited) { throw "$Name exited early with code $($Process.ExitCode)" }
        if (Test-TcpPort -Port $Port) { return }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$Name did not listen within 20 seconds"
}

function Wait-Condition {
    param([scriptblock]$Condition, [string]$Failure, [int]$Seconds = $TimeoutSeconds)
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        if ($null -ne $collectorProcess -and $collectorProcess.HasExited) {
            throw "collector exited early with code $($collectorProcess.ExitCode)"
        }
        $result = & $Condition
        if ($result) { return $result }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $deadline)
    throw $Failure
}

function Stop-TestProcess {
    param([System.Diagnostics.Process]$Process)
    if ($null -ne $Process -and -not $Process.HasExited) {
        Stop-Process -Id $Process.Id
        $Process.WaitForExit(10000) | Out-Null
    }
}

function Start-MockArms {
    $env:MOCK_ARMS_EXPECTED_ACCESS_KEY_ID = 'testbed-access-key-id'
    $env:MOCK_ARMS_DATASET_USERNAME = 'testbed-user'
    $env:MOCK_ARMS_DATASET_PASSWORD = 'testbed-password'
    $process = Start-Process -FilePath $java -ArgumentList @('-jar', $mockArmsJar, '--bind', '127.0.0.1', '--port', $MockArmsPort) `
        -WorkingDirectory $testbed -WindowStyle Hidden -RedirectStandardOutput (Join-Path $runRoot 'smoke-mock-arms.stdout.log') `
        -RedirectStandardError (Join-Path $runRoot 'smoke-mock-arms.stderr.log') -PassThru
    Wait-TcpPort -Port $MockArmsPort -Process $process -Name 'mock-arms'
    return $process
}

function Start-MockCc {
    $env:MOCK_CC_KEYSTORE_PASSWORD = $Password
    $env:MOCK_CC_TRUSTSTORE_PASSWORD = $Password
    $arguments = @('-jar', $mockCcJar, '--bind', '127.0.0.1', '--port', $MockCcPort,
        '--state-dir', (Join-Path $testbed '.state\mock-cc-ingest'),
        '--key-store', (Join-Path $pki 'server.p12'), '--trust-store', (Join-Path $pki 'server-trust.p12'),
        '--collector-cert-map', (Join-Path $pki 'collector-cert-map.json'))
    $process = Start-Process -FilePath $java -ArgumentList $arguments -WorkingDirectory $testbed `
        -WindowStyle Hidden -RedirectStandardOutput (Join-Path $runRoot 'smoke-mock-cc.stdout.log') `
        -RedirectStandardError (Join-Path $runRoot 'smoke-mock-cc.stderr.log') -PassThru
    Wait-TcpPort -Port $MockCcPort -Process $process -Name 'mock-cc-ingest'
    return $process
}

function Start-Collector {
    return Start-Process -FilePath $java -ArgumentList @('-jar', $collectorJar, 'daemon', '--config', $runtimeConfig) `
        -WorkingDirectory $project -WindowStyle Hidden -RedirectStandardOutput (Join-Path $runRoot 'smoke-collector.stdout.log') `
        -RedirectStandardError (Join-Path $runRoot 'smoke-collector.stderr.log') -PassThru
}

function Invoke-MockCc {
    param([string]$Path, [string]$Method = 'GET', [string]$Body = '')
    $env:MOCK_CC_HEALTH_KEYSTORE_PATH = Join-Path $pki 'client-1.p12'
    $env:MOCK_CC_HEALTH_TRUSTSTORE_PATH = Join-Path $pki 'collector-trust.p12'
    $env:MOCK_CC_KEYSTORE_PASSWORD = $Password
    $env:MOCK_CC_TRUSTSTORE_PASSWORD = $Password
    $arguments = @('-cp', $mockCcJar, 'cn.kjky.collector.testbed.cc.MockCcAdminClient',
        '--url', "https://localhost:$MockCcPort$Path", '--method', $Method, '--body', $Body)
    $result = & $java @arguments
    if ($LASTEXITCODE -ne 0) { throw "mock-cc-ingest request failed for $Path" }
    return ($result -join "`n")
}

function Invoke-MockArms {
    param([string]$Path, [string]$Method = 'GET', [string]$Body = '')
    $parameters = @{ Uri = "http://127.0.0.1:$MockArmsPort$Path"; Method = $Method; TimeoutSec = 5 }
    if ($Method -eq 'POST') { $parameters.Body = $Body; $parameters.ContentType = 'application/json' }
    return Invoke-RestMethod @parameters
}

function Get-OutboxCount {
    return @(
        Get-ChildItem -LiteralPath (Join-Path $dataRoot 'outbox\pending') -Directory -ErrorAction SilentlyContinue
        Get-ChildItem -LiteralPath (Join-Path $dataRoot 'outbox\retry') -Directory -ErrorAction SilentlyContinue
    ).Count
}

try {
    if ((Test-TcpPort -Port $MockCcPort) -or (Test-TcpPort -Port $MockArmsPort)) {
        throw 'smoke test requires free mock ports; stop an existing testbed first'
    }
    if (-not (Test-Path -LiteralPath (Join-Path $pki 'server.p12'))) {
        & (Join-Path $PSScriptRoot 'generate-test-pki.ps1') -Password $Password
    }
    & mvn -f (Join-Path $testbed 'pom.xml') -DskipTests package
    if ($LASTEXITCODE -ne 0) { throw 'testbed package failed' }
    & mvn -f (Join-Path $project 'pom.xml') -DskipTests package
    if ($LASTEXITCODE -ne 0) { throw 'collector package failed' }

    if (Test-Path -LiteralPath $smokeRoot) {
        $resolvedSmoke = [System.IO.Path]::GetFullPath($smokeRoot)
        if (-not $resolvedSmoke.StartsWith($testbed + [System.IO.Path]::DirectorySeparatorChar,
                [System.StringComparison]::OrdinalIgnoreCase)) { throw 'refusing to clear smoke state outside testbed' }
        Remove-Item -LiteralPath $smokeRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Path $dataRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $configText = Get-Content -LiteralPath $sourceConfig -Raw
    $configText = $configText.Replace('127.0.0.1:9410', "127.0.0.1:$MockArmsPort")
    $configText = $configText.Replace('localhost:9443', "localhost:$MockCcPort")
    Set-Content -LiteralPath $runtimeConfig -Value $configText -Encoding utf8NoBOM

    $env:TESTBED_KEYSTORE_PASSWORD = $Password
    $env:TESTBED_ARMS_ACCESS_KEY_ID = 'testbed-access-key-id'
    $env:TESTBED_ARMS_ACCESS_KEY_SECRET = 'testbed-access-key-secret'
    $env:TESTBED_ARMS_DATASET_USERNAME = 'testbed-user'
    $env:TESTBED_ARMS_DATASET_PASSWORD = 'testbed-password'
    $env:TESTBED_SLS_ACCESS_KEY_ID = 'testbed-sls-key-id'
    $env:TESTBED_SLS_ACCESS_KEY_SECRET = 'testbed-sls-key-secret'

    $armsProcess = Start-MockArms
    $collectorProcess = Start-Collector

    $null = Wait-Condition -Failure 'collector did not retain Outbox while mock-cc-ingest was down' -Condition {
        if ((Get-OutboxCount) -gt 0) { return $true }
        return $false
    }
    $retainedWhileCcDown = Get-OutboxCount
    if (-not (Test-Path -LiteralPath (Join-Path $dataRoot 'state\collection-watermarks.json'))) {
        throw 'collection watermark was not persisted while cc was unavailable'
    }

    $ccProcess = Start-MockCc
    $null = Invoke-MockCc -Path '/__admin/reset?delete_committed=true' -Method 'POST'
    $baseline = Wait-Condition -Failure 'initial recovery upload did not converge' -Condition {
        $status = (Invoke-MockCc -Path '/__admin/status') | ConvertFrom-Json
        if ($status.received_batches -ge 1 -and (Get-OutboxCount) -eq 0) { return $status }
        return $null
    }

    $beforeDuplicate = [int]$baseline.received_batches
    $null = Invoke-MockArms -Path '/__admin/scenario' -Method 'POST' `
        -Body '{"scenario":"DUPLICATE_RECORDS","times":1,"target_action":"SearchTracesByPage"}'
    $duplicateStatus = Wait-Condition -Failure 'duplicate-record polling did not reach cc-ingest' -Condition {
        $status = (Invoke-MockCc -Path '/__admin/status') | ConvertFrom-Json
        if ($status.received_batches -gt $beforeDuplicate -and (Get-OutboxCount) -eq 0) { return $status }
        return $null
    }
    $received = (Invoke-MockCc -Path '/__admin/received?include_payload=true') | ConvertFrom-Json
    $eventIds = @($received | ForEach-Object { $_.batch.events } | ForEach-Object { $_.event_id })
    $duplicateEventIds = @($eventIds | Group-Object | Where-Object Count -gt 1)
    if ($duplicateEventIds.Count -lt 1) { throw 'duplicate ARMS records did not produce matching stable event_id values' }

    $before503 = [int]$duplicateStatus.received_batches
    $requestCountBefore503 = [int]$duplicateStatus.request_count
    $null = Invoke-MockCc -Path '/__admin/scenario' -Method 'POST' -Body '{"scenario":"SERVICE_UNAVAILABLE","times":2}'
    $null = Wait-Condition -Failure '503 was not observed by mock-cc-ingest' -Condition {
        $status = (Invoke-MockCc -Path '/__admin/status') | ConvertFrom-Json
        if ($status.request_count -ge ($requestCountBefore503 + 2) -and $status.scenario -eq 'NORMAL') { return $status }
        return $null
    }
    $after503 = Wait-Condition -Failure '503 recovery did not automatically drain Outbox' -Condition {
        $status = (Invoke-MockCc -Path '/__admin/status') | ConvertFrom-Json
        if ($status.received_batches -gt $before503 -and (Get-OutboxCount) -eq 0) { return $status }
        return $null
    }

    $duplicatesBeforeDrop = [int]$after503.duplicate_submissions
    $null = Invoke-MockCc -Path '/__admin/scenario' -Method 'POST' -Body '{"scenario":"DROP_AFTER_COMMIT","times":1}'
    $afterDrop = Wait-Condition -Failure 'DROP_AFTER_COMMIT did not converge through ALREADY_COMMITTED' -Condition {
        $status = (Invoke-MockCc -Path '/__admin/status') | ConvertFrom-Json
        if ($status.dropped_after_commit -ge 1 -and $status.duplicate_submissions -gt $duplicatesBeforeDrop -and (Get-OutboxCount) -eq 0) { return $status }
        return $null
    }

    Stop-TestProcess -Process $ccProcess
    $ccProcess = $null
    $null = Wait-Condition -Failure 'no Outbox batch was retained before collector restart' -Condition {
        if ((Get-OutboxCount) -gt 0) { return $true }
        return $false
    }
    $restartPending = Get-OutboxCount
    $restartRetryAttempts = 0
    Get-ChildItem -LiteralPath (Join-Path $dataRoot 'outbox') -Filter 'delivery.json' -File -Recurse -ErrorAction SilentlyContinue |
        ForEach-Object { $restartRetryAttempts += [int]((Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json).attempt_count) }
    Stop-TestProcess -Process $collectorProcess
    $collectorProcess = $null
    $ccProcess = Start-MockCc
    $collectorProcess = Start-Collector
    $finalStatus = Wait-Condition -Failure 'collector restart did not resume original pending/retry batches' -Condition {
        $status = (Invoke-MockCc -Path '/__admin/status') | ConvertFrom-Json
        if ($status.received_batches -gt [int]$afterDrop.received_batches -and (Get-OutboxCount) -eq 0) { return $status }
        return $null
    }

    $uploadWatermark = Join-Path $dataRoot 'state\upload-watermarks.json'
    if (-not (Test-Path -LiteralPath $uploadWatermark)) { throw 'upload watermark was not persisted' }
    $armsStatus = Invoke-MockArms -Path '/__admin/requests'
    $preRestartRetries = [Math]::Max(0, [int]$afterDrop.request_count - [int]$afterDrop.received_batches)
    $newCommitsAfterRestart = [int]$finalStatus.received_batches - [int]$afterDrop.received_batches
    $postRestartRetries = [Math]::Max(0, [int]$finalStatus.request_count - $newCommitsAfterRestart)
    $retryCount = $preRestartRetries + $restartRetryAttempts + $postRestartRetries
    $duplicateSubmissionCount = [int]$afterDrop.duplicate_submissions + [int]$finalStatus.duplicate_submissions
    [ordered]@{
        phase = 'mock-arms to mTLS cc-ingest end-to-end recovery'
        received_batches = [int]$finalStatus.received_batches
        received_events = [int]$finalStatus.received_events
        retry_count = $retryCount
        duplicate_submissions = $duplicateSubmissionCount
        retained_while_cc_down = $retainedWhileCcDown
        restart_pending_batches = $restartPending
        stable_duplicate_event_id_groups = $duplicateEventIds.Count
        arms_requests = [int]$armsStatus.request_count
        remaining_outbox = Get-OutboxCount
        collection_watermark = Join-Path $dataRoot 'state\collection-watermarks.json'
        upload_watermark = $uploadWatermark
        result = 'PASS'
    } | ConvertTo-Json
} finally {
    Stop-TestProcess -Process $collectorProcess
    Stop-TestProcess -Process $ccProcess
    Stop-TestProcess -Process $armsProcess
}
