# Failure drills on the running cluster. Each drill writes with acks=all and checks
# how many messages actually landed, so lost writes and blocked writes are visible.
#   1. one node down        -> writes continue (ISR 3 -> 2)
#   2. two nodes down       -> writes stop (ISR 1 < min.insync.replicas, quorum lost)
#   3. both nodes back      -> ISR 3 again, no acknowledged message lost
#   4. broker process crash -> systemd restarts it (Restart=on-failure)
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$first = $Nodes[0].Name
$second = $Nodes[1].Name
$third = $Nodes[2].Name
$results = New-Object System.Collections.Generic.List[object]

function Drill([string]$cmd) { multipass exec $first -- kafka-drill @($cmd -split ' ') }
# When only the first node is alive, keep it as the only bootstrap server
function DrillAlone([string]$cmd) { multipass exec $first -- env BS="$($first):9092" kafka-drill @($cmd -split ' ') }
function Count([switch]$Alone) {
    if ($Alone) { [int](DrillAlone 'count') } else { [int](Drill 'count') }
}

function Wait-IsrFull([int]$TimeoutSec = 180) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        multipass exec $first -- kafka-drill isr-full
        if ($LASTEXITCODE -eq 0) { return [int]$sw.Elapsed.TotalSeconds }
        Start-Sleep -Seconds 5
    }
    throw "ISR not full after $TimeoutSec s"
}

function Add-Result($name, $sent, $before, $after, $expect) {
    $landed = $after - $before
    $results.Add([pscustomobject]@{
        Drill = $name; Sent = $sent; Landed = $landed; Expected = $expect
        Verdict = $(if ($landed -eq $expect) { 'OK' } else { 'CHECK' })
    })
}

Write-Host "== Preparation"
Drill 'create' | Out-Null
$t = Wait-IsrFull
Write-Host "ISR full ($t s)"

Write-Host "`n== Drill 1: stop $third"
multipass stop $third
Drill 'desc'
$before = Count
Drill 'produce drill1 500'
Add-Result "1 node down" 500 $before (Count) 500

Write-Host "`n== Drill 2: stop $second as well ($first alone, 1 of 3 voters)"
multipass stop $second
DrillAlone 'desc'
DrillAlone 'quorum'
$before = Count -Alone
DrillAlone 'produce drill2 300'
Add-Result "2 nodes down" 300 $before (Count -Alone) 0

Write-Host "`n== Drill 3: start $second and $third"
multipass start $second $third
$t = Wait-IsrFull
Write-Host "ISR full again after $t s (static IPs: no /etc/hosts fix needed)"
$before = Count
Drill 'produce drill3 500'
Add-Result "recovered" 500 $before (Count) 500
multipass exec $first -- env LOG_DIR=/tmp/kafka-cli KAFKA_HEAP_OPTS=-Xmx128m `
    /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server "$($first):9092" `
    --election-type PREFERRED --all-topic-partitions 2>&1 | Out-Null
Drill 'desc'

Write-Host "`n== Drill 4: SIGKILL the broker process on $second"
$pidBefore = multipass exec $second -- systemctl show -p MainPID --value kafka
# Exit code is non-zero because systemd also signals helper processes; the main PID dies
multipass exec $second -- sudo systemctl kill -s SIGKILL kafka 2>&1 | Out-Null
$before = Count
Drill 'produce drill4 300'
$t = Wait-IsrFull
$pidAfter = multipass exec $second -- systemctl show -p MainPID --value kafka
$restarts = multipass exec $second -- systemctl show -p NRestarts --value kafka
Write-Host "PID $pidBefore -> $pidAfter, NRestarts=$restarts, ISR full after $t s"
Add-Result "process crash" 300 $before (Count) 300

Write-Host "`n== Summary"
$results | Format-Table -AutoSize
