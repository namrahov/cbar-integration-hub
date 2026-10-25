# Shared settings for all kafka-vm scripts. Dot-source: . "$PSScriptRoot\..\config.ps1"

$KafkaVersion = '4.3.2'
$ScalaVersion = '2.13'

# 3 nodes in KRaft combined mode (broker + controller on each node).
# Ip = static address on the lab switch; brokers talk to each other over it.
$Nodes = @(
    @{ Name = 'kafka-1'; Id = 1; Rack = 'rack-a'; Ip = '10.10.10.11' },
    @{ Name = 'kafka-2'; Id = 2; Rack = 'rack-b'; Ip = '10.10.10.12' },
    @{ Name = 'kafka-3'; Id = 3; Rack = 'rack-c'; Ip = '10.10.10.13' }
)

# Hyper-V Default Switch gives DHCP addresses that change on every VM restart.
# A second NIC on a private internal switch carries fixed IPs for the cluster;
# the Default Switch NIC stays for internet access (apt, downloads).
$LabSwitch  = 'KafkaLab'
$LabHostIp  = '10.10.10.1'   # Windows side of the switch: lets host clients reach brokers
$LabPrefix  = 24

# VM sizing. 14 GB host RAM -> keep VMs small; Kafka heap is set in kafka.service.
# 1G was too small: broker (~550 MB RSS) + one CLI JVM triggered the OOM killer.
# Existing VMs are not resized by 01-create-vms: stop, `multipass set local.<vm>.memory=...`, start.
$VmImage  = '24.04'
$VmCpus   = 2
$VmMemory = '1536M'
$VmDisk   = '10G'

$NodeDir = Join-Path $PSScriptRoot 'node'
