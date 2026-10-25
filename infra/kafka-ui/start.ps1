# Downloads (once, checksum-verified) and runs Kafbat Kafka UI on http://localhost:8090
# Needs Java 21+ on PATH. Stop with Ctrl+C.
$ErrorActionPreference = 'Stop'

$Version = '1.5.0'
# sha256 of api-v1.5.0.jar as published by GitHub for the release asset
$Sha256  = '8bebff7b21ddb084b5b647e271136f7d97f46da6c7bc70f9cd47775dfbd3c10e'

$libDir = Join-Path $PSScriptRoot 'lib'
$jar    = Join-Path $libDir "api-v$Version.jar"
$config = Join-Path $PSScriptRoot 'application.yml'

if (-not (Test-Path $jar)) {
    New-Item -ItemType Directory -Force $libDir | Out-Null
    $url = "https://github.com/kafbat/kafka-ui/releases/download/v$Version/api-v$Version.jar"
    Write-Host "Downloading $url (~140 MB)"
    $tmp = "$jar.part"
    # PS 5.1: GitHub needs TLS 1.2, and the progress bar slows big downloads ~10x
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $url -OutFile $tmp -UseBasicParsing
    $actual = (Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower()
    if ($actual -ne $Sha256) {
        Remove-Item $tmp
        throw "SHA256 mismatch for api-v$Version.jar: $actual"
    }
    Move-Item $tmp $jar
}

# Login credentials live in a local, git-ignored file. First run generates a random password.
$credFile = Join-Path $PSScriptRoot 'credentials.local'
if (-not (Test-Path $credFile)) {
    $chars = [char[]]'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789'
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    $bytes = New-Object byte[] 24
    $rng.GetBytes($bytes)
    $password = -join ($bytes | ForEach-Object { $chars[$_ % $chars.Length] })
    Set-Content -Path $credFile -Encoding ASCII -Value @("user=admin", "password=$password")
    Write-Host "Generated login credentials in $credFile"
}
$cred = @{}
Get-Content $credFile | ForEach-Object { $k, $v = $_ -split '=', 2; $cred[$k.Trim()] = $v.Trim() }
if (-not $cred.user -or -not $cred.password) { throw "$credFile must contain user=... and password=..." }
$env:KAFKA_UI_USER = $cred.user
$env:KAFKA_UI_PASSWORD = $cred.password

Write-Host "Kafka UI $Version -> http://localhost:8090 (login: see $credFile)"
& java -Xmx384m "-Dspring.config.additional-location=file:$config" -jar $jar
