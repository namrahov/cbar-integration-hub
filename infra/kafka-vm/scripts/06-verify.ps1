# Health check: service state, KRaft quorum, and a replicated test topic.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$first = $Nodes[0].Name
$bootstrap = ($Nodes | ForEach-Object { "$($_.Name):9092" }) -join ','
$bin = '/opt/kafka/bin'

# CLI tools try to create a log dir next to the binaries; point them somewhere writable.
# Small heap: the CLI JVM shares the VM with the broker and must not push it into OOM.
function Invoke-KafkaCli([string]$Tool) {
    multipass exec $first -- env LOG_DIR=/tmp/kafka-cli KAFKA_HEAP_OPTS=-Xmx128m "$bin/$Tool" --bootstrap-server $bootstrap @args
}

foreach ($n in $Nodes) {
    $state = multipass exec $n.Name -- systemctl is-active kafka
    Write-Host ("{0,-8} kafka.service: {1}" -f $n.Name, $state)
}

Write-Host "`n== KRaft quorum (leader, voters, lag) =="
Invoke-KafkaCli kafka-metadata-quorum.sh describe --status
Invoke-KafkaCli kafka-metadata-quorum.sh describe --replication

Write-Host "`n== Test topic: RF=3, min.insync.replicas=2 =="
Invoke-KafkaCli kafka-topics.sh --create --if-not-exists --topic healthcheck `
    --partitions 3 --replication-factor 3 --config min.insync.replicas=2
Invoke-KafkaCli kafka-topics.sh --describe --topic healthcheck

Write-Host "`n== Under-replicated partitions (should be empty) =="
Invoke-KafkaCli kafka-topics.sh --describe --under-replicated-partitions
