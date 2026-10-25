# Formats storage on all nodes with ONE shared cluster id, then starts Kafka.
# Formatting is a one-time operation: already formatted nodes are left alone.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$first = $Nodes[0].Name

# Reuse the cluster id if any node is already formatted, otherwise generate a new one
$clusterId = multipass exec $first -- bash -c "sudo grep -s '^cluster.id=' /var/lib/kafka/data/meta.properties | cut -d= -f2"
if (-not $clusterId) {
    $clusterId = multipass exec $first -- env LOG_DIR=/tmp/kafka-cli /opt/kafka/bin/kafka-storage.sh random-uuid
}
$clusterId = "$clusterId".Trim()
if (-not $clusterId) { throw 'could not obtain cluster id' }
Write-Host "Cluster id: $clusterId"

foreach ($n in $Nodes) {
    Write-Host "[format] $($n.Name)"
    # Run as the kafka user so the data dir stays owned by the service account
    multipass exec $n.Name -- sudo -u kafka env LOG_DIR=/var/log/kafka /opt/kafka/bin/kafka-storage.sh format `
        --config /etc/kafka/server.properties --cluster-id $clusterId --ignore-formatted
    if ($LASTEXITCODE -ne 0) { throw "storage format failed on $($n.Name)" }
}

# Start all nodes together so the controller quorum can elect a leader
foreach ($n in $Nodes) {
    Write-Host "[start] $($n.Name)"
    multipass exec $n.Name -- sudo systemctl enable --now kafka
}
