# Creates the private Hyper-V switch for static cluster IPs and attaches a second NIC
# to every VM that lacks one. Idempotent; needs Administrator only when there is work to do.
# Note: adding a NIC requires stopping the VM, so run this before the cluster carries traffic
# or treat it like a rolling restart.
# Not 'Stop': PS 5.1 turns any native stderr line into a terminating error when
# output is redirected. Failures are detected via $LASTEXITCODE + throw instead.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\..\config.ps1"

function Test-Admin {
    ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).
        IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# Interface count inside the VM is readable without admin rights
function Get-NicCount([string]$vm) {
    $links = multipass exec $vm -- ls /sys/class/net
    ($links -split '\s+' | Where-Object { $_ -match '^(eth|en)' }).Count
}

$hostNic = "vEthernet ($LabSwitch)"
$switchReady = [bool](Get-NetAdapter -Name $hostNic -ErrorAction SilentlyContinue)
$needNic = @($Nodes | Where-Object { (Get-NicCount $_.Name) -lt 2 } | ForEach-Object { $_.Name })

if ($switchReady -and $needNic.Count -eq 0) {
    Write-Host "[skip] switch '$LabSwitch' exists and all VMs have a lab NIC"
    return
}
if (-not (Test-Admin)) {
    throw "Network setup needed (switch ready: $switchReady, VMs without lab NIC: $($needNic -join ', ')). " +
          "Run this script once in PowerShell 'Run as administrator':`n  $PSCommandPath"
}

if (-not $switchReady) {
    Write-Host "[switch] creating internal switch $LabSwitch, host IP $LabHostIp/$LabPrefix"
    New-VMSwitch -Name $LabSwitch -SwitchType Internal | Out-Null
    New-NetIPAddress -InterfaceAlias $hostNic -IPAddress $LabHostIp -PrefixLength $LabPrefix | Out-Null
}

foreach ($vm in $needNic) {
    Write-Host "[nic] $vm : stop -> add adapter on $LabSwitch -> start"
    multipass stop $vm
    Add-VMNetworkAdapter -VMName $vm -SwitchName $LabSwitch
    multipass start $vm
    if ($LASTEXITCODE -ne 0) { throw "failed to restart $vm" }
}
