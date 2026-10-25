# Restarts the brokers one at a time, waiting for every partition to be fully in sync before
# moving on. Use after config changes (04-provision), JVM/agent changes, OS patching.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

foreach ($n in $Nodes) {
    # Check from a node that is not being restarted
    $probe = ($Nodes | Where-Object { $_.Name -ne $n.Name } | Select-Object -First 1).Name
    $sw = [Diagnostics.Stopwatch]::StartNew()
    Write-Host "[restart] $($n.Name)"
    multipass exec $n.Name -- sudo systemctl restart kafka
    do {
        Start-Sleep -Seconds 5
        multipass exec $probe -- sudo kafka-drill isr-full
    } until ($LASTEXITCODE -eq 0 -or $sw.Elapsed.TotalSeconds -gt 300)
    if ($LASTEXITCODE -ne 0) { throw "$($n.Name): ISR not full after 300 s - stopping the rolling restart" }
    Write-Host "[ok] $($n.Name) back, ISR full after $([int]$sw.Elapsed.TotalSeconds) s"
}
