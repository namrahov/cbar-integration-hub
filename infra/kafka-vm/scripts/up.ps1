# Full setup: VMs -> lab network -> static IPs -> install -> format + start -> users/ACLs -> verify
# 02-lab-network needs Administrator on first run (it says so and stops if not elevated).
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'

& "$PSScriptRoot\01-create-vms.ps1"
& "$PSScriptRoot\02-lab-network.ps1"
& "$PSScriptRoot\03-static-ip.ps1"
& "$PSScriptRoot\04-provision.ps1"
& "$PSScriptRoot\05-bootstrap-cluster.ps1"
Start-Sleep -Seconds 15
& "$PSScriptRoot\06-security.ps1"
& "$PSScriptRoot\07-verify.ps1"
