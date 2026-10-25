# Downloads (once, checksum-verified) and starts Prometheus and Grafana on Windows, in the background.
#   Prometheus : http://localhost:9090  (metrics, alert rules)
#   Grafana    : http://localhost:3000  (dashboard "Kafka / Kafka lab cluster"; admin password in secrets.local)
# Both listen on 127.0.0.1 only. Data and logs go to .\data (git-ignored). Stop with stop.ps1.
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\..\common.ps1"

$PrometheusVersion = '3.15.1'
$PrometheusSha256  = 'ba25b5e590520cb9fb028edef99bdc8523244d2d489b8c629e0d453d94fb030b'
$GrafanaVersion    = '13.2.4'
$GrafanaSha256     = '0eaddf3dbafa636e9da92e15fe29062958061dc30a73aa1761a7a36d7d019823'

$lib  = Join-Path $PSScriptRoot 'lib'
$data = Join-Path $PSScriptRoot 'data'
New-Item -ItemType Directory -Force $lib, $data | Out-Null

function Get-Verified([string]$Url, [string]$Sha256, [string]$Dest) {
    if (Test-Path $Dest) { return }
    Write-Host "Downloading $Url"
    # PS 5.1: GitHub needs TLS 1.2, and the progress bar slows big downloads ~10x
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $Url -OutFile "$Dest.part" -UseBasicParsing
    $actual = (Get-FileHash "$Dest.part" -Algorithm SHA256).Hash.ToLower()
    if ($actual -ne $Sha256) { Remove-Item "$Dest.part"; throw "SHA256 mismatch for $Url : $actual" }
    Move-Item "$Dest.part" $Dest
}

function Expand-Once([string]$Zip, [string]$Marker) {
    $existing = Get-ChildItem $lib -Recurse -Filter $Marker -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $existing) {
        Write-Host "Extracting $(Split-Path $Zip -Leaf)"
        Expand-Archive -Path $Zip -DestinationPath $lib -Force
        $existing = Get-ChildItem $lib -Recurse -Filter $Marker | Select-Object -First 1
    }
    $existing.FullName
}

function Test-Running([string]$Name) {
    [bool](Get-Process -Name $Name -ErrorAction SilentlyContinue)
}

# --- Prometheus ---
$promZip = Join-Path $lib "prometheus-$PrometheusVersion.windows-amd64.zip"
Get-Verified "https://github.com/prometheus/prometheus/releases/download/v$PrometheusVersion/prometheus-$PrometheusVersion.windows-amd64.zip" $PrometheusSha256 $promZip
$promExe = Expand-Once $promZip 'prometheus.exe'

if (Test-Running 'prometheus') {
    Write-Host '[skip] Prometheus already running'
} else {
    Start-Process -FilePath $promExe -WindowStyle Hidden -WorkingDirectory $PSScriptRoot `
        -RedirectStandardError (Join-Path $data 'prometheus.log') -ArgumentList @(
            "--config.file=`"$(Join-Path $PSScriptRoot 'prometheus.yml')`"",
            "--storage.tsdb.path=`"$(Join-Path $data 'prometheus')`"",
            '--storage.tsdb.retention.time=7d',
            '--web.listen-address=127.0.0.1:9090',
            '--web.enable-lifecycle'
        )
    Write-Host 'Prometheus -> http://localhost:9090'
}

# --- Grafana ---
$gfZip = Join-Path $lib "grafana-$GrafanaVersion.windows-amd64.zip"
Get-Verified "https://dl.grafana.com/oss/release/grafana-$GrafanaVersion.windows-amd64.zip" $GrafanaSha256 $gfZip
$gfExe  = Expand-Once $gfZip 'grafana.exe'
$gfHome = Split-Path (Split-Path $gfExe -Parent) -Parent

$secretsFile = Join-Path $PSScriptRoot 'secrets.local'
$s = Initialize-Secrets $secretsFile @('GRAFANA_ADMIN_PASSWORD')

if (Test-Running 'grafana') {
    Write-Host '[skip] Grafana already running'
} else {
    # Grafana reads GF_<SECTION>_<KEY> environment variables; child processes inherit them
    $env:GF_PATHS_DATA                     = Join-Path $data 'grafana'
    # Grafana on Windows does not create its data dir itself and exits ("failed to create SQLite database file")
    New-Item -ItemType Directory -Force $env:GF_PATHS_DATA | Out-Null
    $env:GF_PATHS_LOGS                     = Join-Path $data 'grafana-logs'
    $env:GF_PATHS_PROVISIONING             = Join-Path $PSScriptRoot 'grafana\provisioning'
    $env:KAFKA_LAB_DASHBOARDS              = Join-Path $PSScriptRoot 'grafana\dashboards'
    $env:GF_SERVER_HTTP_ADDR               = '127.0.0.1'
    $env:GF_SERVER_HTTP_PORT               = '3000'
    $env:GF_SECURITY_ADMIN_USER            = 'admin'
    $env:GF_SECURITY_ADMIN_PASSWORD        = $s.GRAFANA_ADMIN_PASSWORD
    $env:GF_USERS_ALLOW_SIGN_UP            = 'false'
    $env:GF_AUTH_ANONYMOUS_ENABLED         = 'false'
    $env:GF_ANALYTICS_REPORTING_ENABLED    = 'false'
    $env:GF_ANALYTICS_CHECK_FOR_UPDATES    = 'false'
    $env:GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH = Join-Path $env:KAFKA_LAB_DASHBOARDS 'kafka-cluster.json'
    Start-Process -FilePath $gfExe -WindowStyle Hidden -WorkingDirectory $gfHome `
        -RedirectStandardOutput (Join-Path $data 'grafana.out.log') `
        -RedirectStandardError (Join-Path $data 'grafana.log') -ArgumentList @('server', "--homepath=`"$gfHome`"")
    Write-Host "Grafana    -> http://localhost:3000 (user admin, password GRAFANA_ADMIN_PASSWORD in $secretsFile)"
}
