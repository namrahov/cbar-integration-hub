# Shared settings for all kafka-vm scripts. Dot-source: . "$PSScriptRoot\..\config.ps1"

$KafkaVersion = '4.3.2'
$ScalaVersion = '2.13'

# Prometheus exporters on every node (sha256 as published with each release)
$JmxExporterVersion  = '1.7.0'   # Java agent inside the Kafka JVM -> :7071/metrics
$JmxExporterSha256   = '0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9'
$NodeExporterVersion = '1.12.1'  # OS metrics -> :9100/metrics
$NodeExporterSha256  = 'b51d8a76aa2a9156a55d501aca6276fae09e262259a5e4e831d2c2222f084e63'

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

# SASL/SCRAM accounts. Passwords are generated into the git-ignored secrets file.
#   broker, admin : super users (inter-broker traffic, operator CLI)
#   others        : application accounts, limited by ACLs (06-security.ps1)
$SecretsFile = Join-Path $PSScriptRoot 'secrets.local'
$KafkaSecretKeys = @('BROKER_PASSWORD', 'ADMIN_PASSWORD', 'PRODUCER_PASSWORD', 'CONSUMER_PASSWORD', 'KAFKA_UI_PASSWORD')
