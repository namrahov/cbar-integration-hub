# Creates 3 Ubuntu VMs with Multipass (Hyper-V backend). Skips VMs that already exist.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

$existing = multipass list --format csv | ConvertFrom-Csv | ForEach-Object { $_.Name }

foreach ($n in $Nodes) {
    if ($existing -contains $n.Name) {
        Write-Host "[start] $($n.Name) already exists"
        multipass start $n.Name
        if ($LASTEXITCODE -ne 0) { throw "multipass start failed for $($n.Name)" }
        continue
    }
    Write-Host "[create] $($n.Name) ($VmCpus CPU, $VmMemory RAM, $VmDisk disk)"
    multipass launch $VmImage --name $n.Name --cpus $VmCpus --memory $VmMemory --disk $VmDisk
    if ($LASTEXITCODE -ne 0) { throw "multipass launch failed for $($n.Name)" }
}

multipass list
