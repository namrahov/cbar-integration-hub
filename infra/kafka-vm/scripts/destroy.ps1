# Deletes all cluster VMs and their disks. Data is lost.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$names = $Nodes | ForEach-Object { $_.Name }
$answer = Read-Host "Delete VMs $($names -join ', ') and ALL their data? Type 'yes'"
if ($answer -ne 'yes') { Write-Host 'Aborted.'; return }

multipass delete --purge @names
