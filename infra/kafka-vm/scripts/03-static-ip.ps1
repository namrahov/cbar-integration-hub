# Assigns the fixed lab IPs from config.ps1 and writes /etc/hosts on every VM.
# Kafka configs use hostnames only; hostnames now map to IPs that survive restarts.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$lines = $Nodes | ForEach-Object { "$($_.Ip) $($_.Name)" }

# Write with LF endings so the file is clean inside Linux
$hostsFile = Join-Path $env:TEMP 'kafka-cluster-hosts'
[IO.File]::WriteAllText($hostsFile, (($lines -join "`n") + "`n"))

foreach ($n in $Nodes) {
    Write-Host "[static-ip] $($n.Name) -> $($n.Ip)/$LabPrefix"
    multipass transfer $hostsFile "$($n.Name):/tmp/kafka-cluster-hosts"
    multipass transfer (Join-Path $NodeDir 'set-static-ip.sh') "$($n.Name):/tmp/set-static-ip.sh"
    # Reconfiguring networking can leave the multipass exec session hanging, so the
    # script runs as a transient systemd unit and we poll for the address instead
    multipass exec $n.Name -- sudo systemd-run --unit=kafka-lab-ip --collect `
        bash /tmp/set-static-ip.sh "$($n.Ip)/$LabPrefix" /tmp/kafka-cluster-hosts 2>&1 | Out-Null
    $ok = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 2
        $addr = multipass exec $n.Name -- ip -4 -o addr show
        if ($addr -match [regex]::Escape("$($n.Ip)/")) { $ok = $true; break }
    }
    if (-not $ok) {
        multipass exec $n.Name -- sudo journalctl -u kafka-lab-ip --no-pager -n 20
        throw "static IP $($n.Ip) did not come up on $($n.Name)"
    }
}

# Every node must reach every other node over the lab network
foreach ($n in $Nodes) {
    foreach ($peer in $Nodes) {
        if ($peer.Name -eq $n.Name) { continue }
        multipass exec $n.Name -- ping -c1 -W2 $peer.Name | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "$($n.Name) cannot reach $($peer.Name)" }
    }
}
Write-Host "[static-ip] all nodes reach each other"

Write-Host ""
Write-Host "One time, add these lines to C:\Windows\System32\drivers\etc\hosts (as Administrator)"
Write-Host "so clients on Windows can resolve the advertised listeners. They no longer change:"
Write-Host ""
$lines | ForEach-Object { Write-Host "  $_" }
