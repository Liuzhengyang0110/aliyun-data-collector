[CmdletBinding()]
param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot '..\.generated\pki'),
    [string]$Password = 'testbed-changeit'
)

$ErrorActionPreference = 'Stop'
$testbedRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$output = [System.IO.Path]::GetFullPath($OutputDirectory)
if (-not $output.StartsWith($testbedRoot + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'PKI output must remain below testbed/.generated.'
}

$keytoolCommand = Get-Command keytool -ErrorAction Stop
$keytool = $keytoolCommand.Source

if (Test-Path -LiteralPath $output) {
    Remove-Item -LiteralPath $output -Recurse -Force
}
New-Item -ItemType Directory -Path $output -Force | Out-Null

function Invoke-Keytool {
    param([string[]]$Arguments)
    & $keytool @Arguments | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "keytool failed with exit code $LASTEXITCODE" }
}

function New-CertificateAuthority {
    param([string]$Prefix, [string]$CommonName)
    $store = Join-Path $output "$Prefix-ca.p12"
    $certificate = Join-Path $output "$Prefix-ca.crt"
    Invoke-Keytool @('-genkeypair', '-alias', "$Prefix-ca", '-dname', "CN=$CommonName", '-keyalg', 'RSA',
        '-keysize', '2048', '-sigalg', 'SHA256withRSA', '-validity', '3650', '-ext', 'BC:c',
        '-keystore', $store, '-storetype', 'PKCS12', '-storepass', $Password, '-keypass', $Password, '-noprompt')
    Invoke-Keytool @('-exportcert', '-alias', "$Prefix-ca", '-keystore', $store, '-storepass', $Password,
        '-rfc', '-file', $certificate)
    return @{ Store = $store; Certificate = $certificate; Alias = "$Prefix-ca" }
}

function New-SignedIdentity {
    param(
        [hashtable]$Authority,
        [string]$Name,
        [string]$CommonName,
        [string]$ExtendedKeyUsage,
        [string]$San = '',
        [string]$StartDate = '',
        [int]$ValidityDays = 825
    )
    $store = Join-Path $output "$Name.p12"
    $csr = Join-Path $output "$Name.csr"
    $signed = Join-Path $output "$Name.crt"
    Invoke-Keytool @('-genkeypair', '-alias', $Name, '-dname', "CN=$CommonName", '-keyalg', 'RSA',
        '-keysize', '2048', '-sigalg', 'SHA256withRSA', '-validity', '825',
        '-keystore', $store, '-storetype', 'PKCS12', '-storepass', $Password, '-keypass', $Password, '-noprompt')
    Invoke-Keytool @('-certreq', '-alias', $Name, '-keystore', $store, '-storepass', $Password,
        '-file', $csr, '-sigalg', 'SHA256withRSA')
    $sign = @('-gencert', '-alias', $Authority.Alias, '-keystore', $Authority.Store, '-storepass', $Password,
        '-infile', $csr, '-outfile', $signed, '-rfc', '-validity', $ValidityDays,
        '-ext', "EKU=$ExtendedKeyUsage", '-ext', 'KU=digitalSignature,keyEncipherment')
    if ($San) { $sign += @('-ext', "SAN=$San") }
    if ($StartDate) { $sign += @('-startdate', $StartDate) }
    Invoke-Keytool $sign
    Invoke-Keytool @('-importcert', '-alias', $Authority.Alias, '-file', $Authority.Certificate,
        '-keystore', $store, '-storepass', $Password, '-noprompt')
    Invoke-Keytool @('-importcert', '-alias', $Name, '-file', $signed,
        '-keystore', $store, '-storepass', $Password, '-noprompt')
    Remove-Item -LiteralPath $csr -Force
    return $store
}

function New-TrustStore {
    param([hashtable]$Authority, [string]$Name)
    $store = Join-Path $output "$Name.p12"
    Invoke-Keytool @('-importcert', '-alias', $Authority.Alias, '-file', $Authority.Certificate,
        '-keystore', $store, '-storetype', 'PKCS12', '-storepass', $Password, '-noprompt')
    return $store
}

$trusted = New-CertificateAuthority -Prefix 'testbed' -CommonName 'collector-testbed-ca'
$untrusted = New-CertificateAuthority -Prefix 'untrusted' -CommonName 'collector-testbed-untrusted-ca'

$null = New-SignedIdentity -Authority $trusted -Name 'server' -CommonName 'mock-cc-ingest' `
    -ExtendedKeyUsage 'serverAuth' -San 'dns:localhost,dns:mock-cc-ingest,ip:127.0.0.1'
$null = New-SignedIdentity -Authority $trusted -Name 'server-wrong-host' -CommonName 'wrong-host.invalid' `
    -ExtendedKeyUsage 'serverAuth' -San 'dns:wrong-host.invalid'
$null = New-SignedIdentity -Authority $untrusted -Name 'server-untrusted' -CommonName 'localhost' `
    -ExtendedKeyUsage 'serverAuth' -San 'dns:localhost,ip:127.0.0.1'
$clientOne = New-SignedIdentity -Authority $trusted -Name 'client-1' -CommonName 'testbed-collector-01' `
    -ExtendedKeyUsage 'clientAuth'
$null = New-SignedIdentity -Authority $trusted -Name 'client-2' -CommonName 'testbed-collector-rotated' `
    -ExtendedKeyUsage 'clientAuth'
$null = New-SignedIdentity -Authority $trusted -Name 'client-expired' -CommonName 'testbed-collector-expired' `
    -ExtendedKeyUsage 'clientAuth' -StartDate '2020/01/01 00:00:00' -ValidityDays 1
$null = New-SignedIdentity -Authority $untrusted -Name 'client-untrusted' -CommonName 'testbed-collector-untrusted' `
    -ExtendedKeyUsage 'clientAuth'

$null = New-TrustStore -Authority $trusted -Name 'server-trust'
$null = New-TrustStore -Authority $trusted -Name 'collector-trust'
$null = New-TrustStore -Authority $untrusted -Name 'untrusted-server-trust'

$certificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($clientOne, $Password)
try {
    $fingerprint = $certificate.GetCertHashString([System.Security.Cryptography.HashAlgorithmName]::SHA256).ToLowerInvariant()
} finally {
    $certificate.Dispose()
}
$mapping = @{ 'testbed-collector-01' = $fingerprint } | ConvertTo-Json
Set-Content -LiteralPath (Join-Path $output 'collector-cert-map.json') -Value $mapping -Encoding utf8NoBOM

Write-Host "Generated test-only PKI under $output"
Write-Host 'The directory is git-ignored. Never use these credentials outside the testbed.'
