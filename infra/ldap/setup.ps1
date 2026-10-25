# Installs lldap on kafka-1 and creates the Kafka UI groups/accounts. Idempotent.
# Lab compromise: no RAM for a separate VM, so the directory runs next to a broker.
# Secrets are generated once into secrets.local (git-ignored) and copied to the VM.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\kafka-vm\config.ps1"

$LldapVersion = '0.6.3'
# sha256 of amd64-lldap.tar.gz as published by GitHub for the release asset
$LldapSha256  = 'e80b50ef39be61de9f1d75182b1e1fa5d82c672261c7e34fd8ffec5455ed3883'
$Vm   = $Nodes[0].Name
$Ip   = $Nodes[0].Ip
$Node = Join-Path $PSScriptRoot 'node'

. "$PSScriptRoot\..\common.ps1"

$secretsFile = Join-Path $PSScriptRoot 'secrets.local'
$s = Initialize-Secrets $secretsFile @('ADMIN_PASSWORD', 'BIND_PASSWORD', 'VIEWER_PASSWORD', 'LLDAP_JWT_SECRET', 'LLDAP_KEY_SEED')

Write-Host "[lldap] copying files to $Vm"
multipass exec $Vm -- bash -c 'rm -rf /tmp/lldap-node && mkdir -p /tmp/lldap-node'
Get-ChildItem $Node -File | ForEach-Object {
    multipass transfer $_.FullName "${Vm}:/tmp/lldap-node/$($_.Name)"
}
$envFile = Join-Path $env:TEMP 'lldap.env'
Write-LfFile $envFile @(
    "LLDAP_JWT_SECRET=$($s.LLDAP_JWT_SECRET)",
    "LLDAP_KEY_SEED=$($s.LLDAP_KEY_SEED)",
    "LLDAP_LDAP_USER_PASS=$($s.ADMIN_PASSWORD)"
)
multipass transfer $envFile "${Vm}:/tmp/lldap-node/lldap.env"
Remove-Item $envFile

Write-Host "[lldap] installing on $Vm ($Ip)"
multipass exec $Vm -- sudo bash /tmp/lldap-node/install-lldap.sh $LldapVersion $LldapSha256 $Ip /tmp/lldap-node/lldap.env
if ($LASTEXITCODE -ne 0) { throw "lldap install failed on $Vm" }

Write-Host "[lldap] groups and accounts"
$bootFile = Join-Path $env:TEMP 'lldap-bootstrap.env'
Write-LfFile $bootFile @(
    "ADMIN_PASSWORD='$($s.ADMIN_PASSWORD)'",
    "BIND_PASSWORD='$($s.BIND_PASSWORD)'",
    "VIEWER_PASSWORD='$($s.VIEWER_PASSWORD)'"
)
multipass transfer $bootFile "${Vm}:/tmp/lldap-node/bootstrap.env"
Remove-Item $bootFile
multipass exec $Vm -- sudo bash /tmp/lldap-node/bootstrap-lldap.sh "http://${Ip}:17170" /tmp/lldap-node/bootstrap.env
if ($LASTEXITCODE -ne 0) { throw "lldap bootstrap failed on $Vm" }

Write-Host ""
Write-Host "lldap web UI : http://${Ip}:17170  (user: admin, password: ADMIN_PASSWORD in $secretsFile)"
Write-Host "LDAP         : ldap://${Ip}:3890, base dc=cbar,dc=lab"
