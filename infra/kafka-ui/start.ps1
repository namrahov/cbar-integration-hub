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

# Users log in with LDAP accounts. The UI itself only needs the bind (service account) password,
# which infra/ldap/setup.ps1 generated into a git-ignored file.
$secretsFile = Join-Path $PSScriptRoot '..\ldap\secrets.local'
if (-not (Test-Path $secretsFile)) { throw "Run infra\ldap\setup.ps1 first ($secretsFile missing)" }
$bind = Get-Content $secretsFile | Where-Object { $_ -like 'BIND_PASSWORD=*' } | ForEach-Object { ($_ -split '=', 2)[1].Trim() }
if (-not $bind) { throw "BIND_PASSWORD missing in $secretsFile" }
$env:KAFKA_UI_LDAP_BIND_PASSWORD = $bind

Write-Host "Kafka UI $Version -> http://localhost:8090 (log in with an LDAP account)"
& java -Xmx384m "-Dspring.config.additional-location=file:$config" -jar $jar
