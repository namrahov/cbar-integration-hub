# Formats storage on all nodes with ONE shared cluster id, then starts Kafka.
# Formatting is a one-time operation: already formatted nodes are left alone.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$first = $Nodes[0].Name

# Reuse the cluster id if any node is already formatted, otherwise generate a new one
$meta = multipass exec $first -- sudo cat /var/lib/kafka/data/meta.properties 2>$null
$clusterId = ($meta | Where-Object { $_ -like 'cluster.id=*' }) -replace '^cluster.id=', ''
if (-not $clusterId) {
    $clusterId = multipass exec $first -- env LOG_DIR=/tmp/kafka-cli /opt/kafka/bin/kafka-storage.sh random-uuid
}
$clusterId = "$clusterId".Trim()
if (-not $clusterId) { throw 'could not obtain cluster id' }
Write-Host "Cluster id: $clusterId"

foreach ($n in $Nodes) {
    Write-Host "[format] $($n.Name)"
    multipass exec $n.Name -- sudo bash /tmp/kafka-node/format-storage.sh $clusterId
    if ($LASTEXITCODE -ne 0) { throw "storage format failed on $($n.Name)" }
}

# Start all nodes together so the controller quorum can elect a leader
foreach ($n in $Nodes) {
    Write-Host "[start] $($n.Name)"
    multipass exec $n.Name -- sudo systemctl enable --now kafka 2>&1 | Out-Null
}
