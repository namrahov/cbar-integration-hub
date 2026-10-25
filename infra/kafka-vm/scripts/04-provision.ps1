# Copies node files to each VM and runs install-kafka.sh (idempotent).
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

foreach ($n in $Nodes) {
    Write-Host "[provision] $($n.Name)"
    multipass exec $n.Name -- bash -c 'rm -rf /tmp/kafka-node && mkdir -p /tmp/kafka-node'
    Get-ChildItem $NodeDir -File | ForEach-Object {
        multipass transfer $_.FullName "$($n.Name):/tmp/kafka-node/$($_.Name)"
        if ($LASTEXITCODE -ne 0) { throw "transfer of $($_.Name) to $($n.Name) failed" }
    }
    multipass exec $n.Name -- sudo bash /tmp/kafka-node/install-kafka.sh `
        $KafkaVersion $ScalaVersion $n.Id $n.Name $n.Rack
    if ($LASTEXITCODE -ne 0) { throw "install-kafka.sh failed on $($n.Name)" }
}
