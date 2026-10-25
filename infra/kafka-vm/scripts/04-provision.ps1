# Copies node files to each VM and runs install-kafka.sh (idempotent).
# Renders server.properties only; a running broker picks changes up after a restart.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"
. "$PSScriptRoot\..\..\common.ps1"

$s = Initialize-Secrets $SecretsFile $KafkaSecretKeys
$nodeSecrets = Join-Path $env:TEMP 'kafka-node-secrets.env'

foreach ($n in $Nodes) {
    Write-Host "[provision] $($n.Name)"
    multipass exec $n.Name -- bash -c 'rm -rf /tmp/kafka-node && mkdir -p /tmp/kafka-node'
    Get-ChildItem $NodeDir -File | ForEach-Object {
        multipass transfer $_.FullName "$($n.Name):/tmp/kafka-node/$($_.Name)"
        if ($LASTEXITCODE -ne 0) { throw "transfer of $($_.Name) to $($n.Name) failed" }
    }
    # Only the two super-user passwords go to the brokers; app passwords never leave Windows
    Write-LfFile $nodeSecrets @("BROKER_PASSWORD=$($s.BROKER_PASSWORD)", "ADMIN_PASSWORD=$($s.ADMIN_PASSWORD)")
    multipass transfer $nodeSecrets "$($n.Name):/tmp/kafka-node/secrets.env"
    Remove-Item $nodeSecrets
    multipass exec $n.Name -- sudo bash /tmp/kafka-node/install-kafka.sh `
        $KafkaVersion $ScalaVersion $n.Id $n.Name $n.Rack /tmp/kafka-node/secrets.env
    if ($LASTEXITCODE -ne 0) { throw "install-kafka.sh failed on $($n.Name)" }
}
