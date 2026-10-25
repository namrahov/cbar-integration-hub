# Monitoring: Prometheus + Grafana

```
kafka-1..3  : JMX exporter (Java agent in the broker JVM) :7071  ─┐
              node_exporter (OS)                          :9100  ─┤
Windows     : kafka-producer        /actuator/prometheus  :8081  ─┼─> Prometheus 127.0.0.1:9090 ─> Grafana 127.0.0.1:3000
              cbar-integration-hub  /actuator/prometheus  :8082  ─┘        (alert rules)
```

| Component | Version | Where | Installed by |
|---|---|---|---|
| JMX exporter (javaagent) | 1.7.0 | every broker, `/opt/jmx-exporter`, rules `/etc/kafka/jmx-exporter.yml` | `infra/kafka-vm/scripts/04-provision.ps1` |
| node_exporter | 1.12.1 | every VM, systemd `node_exporter` | `infra/kafka-vm/scripts/04-provision.ps1` |
| Prometheus | 3.15.1 | Windows, `infra/monitoring/lib` | `start.ps1` |
| Grafana OSS | 13.2.4 | Windows, `infra/monitoring/lib` | `start.ps1` |
| Micrometer Prometheus registry | Boot-managed | both Spring Boot apps | `build.gradle` |

All downloads are SHA256-verified. Binaries, collected data and the Grafana admin password
(`secrets.local`) are git-ignored.

## Run

```powershell
.\infra\monitoring\start.ps1   # downloads once, starts both in the background
.\infra\monitoring\stop.ps1
```

- Prometheus: http://localhost:9090 → *Status → Targets*, *Alerts*
- Grafana: http://localhost:3000, user `admin`, password `GRAFANA_ADMIN_PASSWORD` in
  `infra/monitoring/secrets.local`. Home dashboard: **Kafka / Kafka lab cluster**.

The JMX agent is wired into `kafka.service` (`KAFKA_OPTS=-javaagent:...`), so installing it on a
running cluster needs a broker restart: `infra/kafka-vm/scripts/rolling-restart.ps1` (one broker at
a time, waits for full ISR).

## Dashboard

| Row | Panels |
|---|---|
| Cluster health | Brokers up, active controller, under-replicated, under min ISR, offline partitions, failed logins, partitions, topics, fenced brokers, firing alerts, producer send errors, max consumer lag |
| Replication | URP per broker, ISR shrinks/expands, leaders and partitions per broker |
| Throughput & latency | Messages in per topic, bytes in/out, Produce p99, request handler idle |
| JVM & hosts | Broker heap, host CPU, memory available, disk used |
| Applications | Producer sent/failed per second, consumer lag and consumed per second |

## Alerts (`rules/kafka-alerts.yml`)

| Alert | Severity | When |
|---|---|---|
| KafkaBrokerDown | critical | broker metrics endpoint unreachable 1 min |
| KafkaOfflinePartitions | critical | a partition has no leader |
| KafkaUnderMinIsr | critical | partitions below `min.insync.replicas` (writes fail) |
| KafkaActiveControllerCount | critical | ≠ 1 (quorum lost / split brain) |
| KafkaUnderReplicatedPartitions | warning | URP > 0 for 5 min |
| KafkaIsrFlapping | warning | ISR keeps shrinking for 10 min |
| KafkaBrokerRestarted | warning | broker process restarted in the last 15 min (crash, OOM, or planned) |
| KafkaAuthenticationFailures | warning | failed SASL logins |
| NodeExporterDown / NodeDiskAlmostFull / NodeMemoryLow | critical / warning | VM down, disk > 75 %, memory < 10 % |
| ProducerSendErrors | critical | the producer app failed to send (possible message loss) |
| ConsumerLagHigh | warning | consumer lag > 1000 for 5 min |
| KafkaAppDown | info | an app is not running |

No Alertmanager in the lab: alerts are shown in Prometheus and Grafana. In production they go
through Alertmanager to e-mail / Teams / on-call.

## Metric names

JMX exporter (see `infra/kafka-vm/node/jmx-exporter.yml`): `kafka_server_replicamanager_*`,
`kafka_controller_*`, `kafka_server_brokertopicmetrics_*_total{topic}`,
`kafka_network_requestmetrics_totaltimems{request,quantile}`,
`kafka_server_authentication_{successful,failed}_total`, `kafka_raft_*`, `jvm_*`.
Spring Boot (Micrometer): `kafka_producer_record_send_total`, `kafka_producer_record_error_total`,
`kafka_consumer_fetch_manager_records_lag_max`, `kafka_consumer_fetch_manager_records_consumed_total`.
