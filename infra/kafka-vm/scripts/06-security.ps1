# Application users (SASL/SCRAM) + ACLs, and a kafka-credentials.properties for each app project.
# Idempotent: re-running keeps passwords (they live in secrets.local) and re-applies ACLs.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"
. "$PSScriptRoot\..\..\common.ps1"

$s = Initialize-Secrets $SecretsFile $KafkaSecretKeys
$first = $Nodes[0].Name

$tmp = Join-Path $env:TEMP 'kafka-app-secrets.env'
Write-LfFile $tmp @(
    "PRODUCER_PASSWORD=$($s.PRODUCER_PASSWORD)",
    "CONSUMER_PASSWORD=$($s.CONSUMER_PASSWORD)",
    "KAFKA_UI_PASSWORD=$($s.KAFKA_UI_PASSWORD)"
)
multipass transfer (Join-Path $NodeDir 'kafka-security.sh') "${first}:/tmp/kafka-security.sh"
multipass transfer $tmp "${first}:/tmp/kafka-app-secrets.env"
Remove-Item $tmp
multipass exec $first -- sudo bash /tmp/kafka-security.sh /tmp/kafka-app-secrets.env
if ($LASTEXITCODE -ne 0) { throw "kafka-security.sh failed on $first" }

# The apps keep their username/password directly in application.yml (lab decision).
# If PRODUCER_PASSWORD / CONSUMER_PASSWORD in secrets.local change, update those files too.
