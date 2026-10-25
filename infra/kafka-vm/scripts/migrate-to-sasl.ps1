# One-off: moves an already running PLAINTEXT cluster to SASL/SCRAM + ACLs.
# (A fresh cluster built by up.ps1 is SASL from the start; this script is not needed there.)
#
# Lab procedure = short planned downtime: all brokers restart together, because a broker that
# speaks SASL to its peers cannot replicate from peers still on PLAINTEXT.
# Zero-downtime production procedure, one rolling restart per phase:
#   1. add a SASL listener next to PLAINTEXT (inter-broker stays PLAINTEXT)
#   2. switch inter.broker.listener.name to the SASL listener
#   3. move clients, enable the authorizer, remove the PLAINTEXT listener
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"
. "$PSScriptRoot\..\..\common.ps1"

$s = Initialize-Secrets $SecretsFile $KafkaSecretKeys
$first = $Nodes[0].Name
$plainBootstrap = ($Nodes | ForEach-Object { "$($_.Name):9092" }) -join ','

$current = multipass exec $first -- sudo grep -c '^listeners=SASL_PLAINTEXT' /etc/kafka/server.properties
if ("$current".Trim() -ne '0') {
    Write-Host "[skip] cluster already configured for SASL"
} else {
    # 1. Super-user SCRAM credentials must exist in the metadata before brokers require them
    foreach ($u in @(@{ Name = 'broker'; Pw = $s.BROKER_PASSWORD }, @{ Name = 'admin'; Pw = $s.ADMIN_PASSWORD })) {
        Write-Host "[scram] $($u.Name) (over PLAINTEXT, last time)"
        multipass exec $first -- env LOG_DIR=/tmp/kafka-cli KAFKA_HEAP_OPTS=-Xmx128m `
            /opt/kafka/bin/kafka-configs.sh --bootstrap-server $plainBootstrap --alter `
            --entity-type users --entity-name $u.Name `
            --add-config "SCRAM-SHA-512=[iterations=8192,password=$($u.Pw)]" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "could not create SCRAM user $($u.Name)" }
    }

    # 2. Render SASL + authorizer config on every node (not active until restart)
    & "$PSScriptRoot\04-provision.ps1"

    # 3. Restart all brokers together. Stop them in parallel: stopped one by one, each broker's
    # controlled shutdown waits for a controller quorum that is already gone and runs into
    # TimeoutStopSec (120 s, then SIGKILL).
    Write-Host "[restart] stopping all brokers"
    foreach ($n in $Nodes) { multipass exec $n.Name -- sudo systemctl stop --no-block kafka }
    foreach ($n in $Nodes) {
        do { Start-Sleep -Seconds 2; $st = multipass exec $n.Name -- systemctl is-active kafka } while ($st -eq 'deactivating')
    }
    Write-Host "[restart] starting all brokers"
    foreach ($n in $Nodes) { multipass exec $n.Name -- sudo systemctl start kafka }
}

# 4. Wait until the cluster is healthy again over SASL
$sw = [Diagnostics.Stopwatch]::StartNew()
do {
    Start-Sleep -Seconds 5
    multipass exec $first -- sudo kafka-drill isr-full
} until ($LASTEXITCODE -eq 0 -or $sw.Elapsed.TotalSeconds -gt 240)
if ($LASTEXITCODE -ne 0) { throw 'cluster did not become healthy over SASL within 240 s' }
Write-Host "[ok] all partitions in sync over SASL after $([int]$sw.Elapsed.TotalSeconds) s"

# 5. Application users and ACLs
& "$PSScriptRoot\06-security.ps1"
