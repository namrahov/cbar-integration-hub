# Kafka 3-node cluster on VMs (KRaft)

3 Ubuntu 24.04 VMs (Multipass + Hyper-V), Kafka 4.3.2, KRaft combined mode,
installed as a systemd service — the way it runs on bare metal / VMs in production.

| VM      | node.id | broker.rack | Static IP   | Ports                        |
|---------|---------|-------------|-------------|------------------------------|
| kafka-1 | 1       | rack-a      | 10.10.10.11 | 9092 client, 9093 controller |
| kafka-2 | 2       | rack-b      | 10.10.10.12 | 9092 client, 9093 controller |
| kafka-3 | 3       | rack-c      | 10.10.10.13 | 9092 client, 9093 controller |

## Network

Each VM has two NICs:

| NIC  | Switch                    | Addressing                | Used for                       |
|------|---------------------------|---------------------------|--------------------------------|
| eth0 | Hyper-V `Default Switch`  | DHCP, **changes on restart** | internet (apt, downloads)   |
| eth1 | Hyper-V `KafkaLab` (internal) | static `10.10.10.0/24` | broker, controller and client traffic |

Windows gets `10.10.10.1` on the `KafkaLab` switch, so host clients reach the brokers directly.
Brokers must have stable addresses: a broker whose IP changes after a restart cannot rejoin
until name resolution is fixed (seen in drill 3 before the lab switch existed).

## Prerequisites (one time, as Administrator)

1. Enable Hyper-V (needs reboot):
   `Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All`
2. Install Multipass: `winget install Canonical.Multipass`
3. RAM: 3 × 1 GB VMs. Close Docker Desktop and heavy apps first (host has 14 GB).
4. Add to `C:\Windows\System32\drivers\etc\hosts` (stable, never needs updating):
   ```
   10.10.10.11 kafka-1
   10.10.10.12 kafka-2
   10.10.10.13 kafka-3
   ```

## Run

```powershell
.\scripts\up.ps1
```

| Step                      | Admin        | What                                                   |
|---------------------------|--------------|--------------------------------------------------------|
| `01-create-vms`           | no           | Multipass VMs; starts stopped ones                     |
| `02-lab-network`          | first run    | `KafkaLab` switch + second NIC on each VM (stops/starts VMs) |
| `03-static-ip`            | no           | netplan static IP on eth1, `/etc/hosts`, ping mesh     |
| `04-provision`            | no           | Java, Kafka, user, dirs, sysctl, systemd, `kafka-drill` |
| `05-bootstrap-cluster`    | no           | one cluster id, `kafka-storage format`, start services |
| `06-verify`               | no           | service state, quorum, RF=3 test topic, URP            |
| `07-drills`               | no           | failure drills (below), not part of `up.ps1`           |

`02-lab-network` checks first and only needs elevation when it has work to do. Run it once
in "Run as administrator" PowerShell; after that `up.ps1` runs as a normal user.

## Layout on each node

| Path                                  | What                                            |
|---------------------------------------|-------------------------------------------------|
| `/opt/kafka_2.13-<ver>`, `/opt/kafka` | binaries; `/opt/kafka` symlink = active version |
| `/etc/kafka/server.properties`        | rendered from `node/server.properties.tmpl`     |
| `/var/lib/kafka/data`                 | partition data + `__cluster_metadata`           |
| `/var/log/kafka`                      | `server.log`, `controller.log`, GC logs         |
| `/etc/systemd/system/kafka.service`   | service unit (heap, ulimits, restart policy)    |
| `/etc/sysctl.d/99-kafka.conf`         | kernel tuning                                   |
| `/etc/netplan/60-kafka-lab.yaml`      | static IP on the lab NIC (matched by MAC)       |
| `/usr/local/bin/kafka-drill`          | drill helper (`node/kafka-drill.sh`)            |

## Day-2 operations

```bash
multipass shell kafka-1
sudo systemctl status kafka
sudo journalctl -u kafka -f
sudo tail -f /var/log/kafka/server.log
kafka-drill desc        # partitions, leaders, ISR of the drill topic
```

Config change → edit template → `04-provision.ps1` → **rolling restart**, one node at a time,
waiting until under-replicated partitions are 0 before the next:

```powershell
multipass exec kafka-1 -- sudo systemctl restart kafka
.\scripts\06-verify.ps1
```

Upgrade → set `$KafkaVersion` in `config.ps1` → `04-provision.ps1` (new dir, symlink switch) → rolling restart.

## Failure drills

```powershell
.\scripts\07-drills.ps1
```

Writes with `acks=all` + idempotence to topic `drill` (RF=3, min.insync.replicas=2) and compares
messages sent with messages that landed (sum of end offsets).

| # | Drill                         | Expected                                                              |
|---|-------------------------------|-----------------------------------------------------------------------|
| 1 | `kafka-3` stopped             | ISR 3 → 2, writes continue                                            |
| 2 | `kafka-2` stopped too         | ISR 1 < min.insync.replicas, 0 messages land, producer blocks         |
| 3 | both started again            | ISR back to 3, nothing acknowledged is lost, preferred leaders restored |
| 4 | broker process `SIGKILL`ed    | systemd restarts it (`NRestarts` +1), writes continue meanwhile       |

What the first manual run showed:

- **Drill 1, 1 of 500 lost on the client side**: `Topic drill not present in metadata after 10000 ms`.
  The producer's first metadata request went to the stopped node, which does not refuse TCP — it
  just doesn't answer. The console producer logs the error and moves on. A real application must
  handle the `send()` callback error (retry, alert, DLQ) or the message silently disappears.
- **Drill 2, the producer hung instead of failing** and had to be killed. Applications need their
  own timeouts / circuit breakers, and monitoring must catch this state.
- **Drill 3, DHCP IPs blocked recovery** until `/etc/hosts` was fixed → static IPs (this setup).
- Leaders stay skewed after recovery until `auto.leader.rebalance` (≤5 min) or
  `kafka-leader-election --election-type PREFERRED`.
- **1 GB VMs → OOM killer** killed the broker when a CLI JVM ran next to it; systemd restarted
  it. VMs are now 1.5 GB (resized with a rolling restart) and CLI tools run with `-Xmx128m`.

Second run (static IPs, 1.5 GB VMs, `07-drills.ps1`): all four drills OK — 500/500, 0/300,
500/500, 300/300. In drill 2 the producer now fails fast with `NOT_ENOUGH_REPLICAS`
(300 send errors in 2 s) instead of hanging.

## Not yet covered (next steps)

- TLS + SASL/SCRAM listeners, ACLs
- Monitoring: JMX exporter → Prometheus → Grafana
- Dedicated controller nodes, DR cluster with MirrorMaker 2
- Ansible instead of shell scripts
