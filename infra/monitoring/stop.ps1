# Stops Prometheus and Grafana started by start.ps1. Collected data stays in .\data.
foreach ($name in 'prometheus', 'grafana') {
    $p = Get-Process -Name $name -ErrorAction SilentlyContinue
    if ($p) { $p | Stop-Process; Write-Host "stopped $name" } else { Write-Host "$name not running" }
}
