# cbar-integration-hub

Lab for secure, reliable and scalable integration with Apache Kafka and MQ.
This repo is the **consumer** side; messages are published by the sibling project
[`kafka-producer`](../kafka-producer).

| Module                                     | What                                       |
|--------------------------------------------|--------------------------------------------|
| [infra/kafka-vm](infra/kafka-vm/README.md) | 3-node Kafka KRaft cluster on VMs, systemd |
| `infra/kafka-ui`                           | Kafbat Kafka UI 1.5.0 on Windows: `.\infra\kafka-ui\start.ps1` → http://localhost:8090 |
| `src/` (Spring Boot 4.1, Java 25)          | `@KafkaListener` on `cbar.messages`        |

## Flow

```
POST :8081/api/messages ──> kafka-producer ──acks=all──> cbar.messages (3 partitions, RF=3, minISR=2)
                                                                │
                                         cbar-integration-hub <─┘  group cbar-integration-hub, 3 threads
```

- The **producer owns the topic**: `KafkaTopicConfig` declares it with `TopicBuilder`, and
  `KafkaAdmin` creates it on startup (`spring.kafka.admin.fail-fast=true`).
- Same key → same partition → ordered processing for that key. No key → spread across partitions.
- Consumer commits after each record (`ack-mode: record`, auto-commit off): at-least-once.
- Failed records are retried 3× (1 s apart), then logged and skipped (`DefaultErrorHandler`).

## Run

Cluster must be up and `kafka-1..3` in the Windows hosts file (see infra README).

```powershell
cd ..\kafka-producer; .\gradlew bootRun     # first: creates the topic
cd ..\cbar-integration-hub; .\gradlew bootRun
```

```powershell
curl.exe -X POST localhost:8081/api/messages -H "Content-Type: application/json" -d '{\"key\":\"ACC-1001\",\"payload\":\"hello\"}'
```

Response: `{"topic":"cbar.messages","partition":1,"offset":0}`; the consumer logs the same partition/offset.
