# Running the Kafka cluster in a real environment (3 servers) — a guide for DevOps

> Azerbaijani version: [PRODUCTION-SETUP.md](PRODUCTION-SETUP.md)

This document walks through what we built in the lab (one laptop, 3 VMs) and, for every step,
shows **what it becomes in a real environment** and **what to ask the DevOps / infrastructure /
network / security teams for**.

> Lab code: `infra/kafka-vm` (cluster), `infra/kafka-ui` (UI), `infra/ldap` (LDAP),
> `src/` (consumer), separate repo `kafka-producer` (producer).

---

## Contents

1. [Big picture](#1-big-picture)
2. [Architecture decisions](#2-architecture-decisions)
3. [Servers (compute, disk, OS)](#3-servers-compute-disk-os)
4. [Network, DNS, firewall](#4-network-dns-firewall)
5. [Installation](#5-installation)
6. [Broker configuration](#6-broker-configuration)
7. [Security](#7-security)
8. [First start of the cluster (bootstrap)](#8-first-start-of-the-cluster-bootstrap)
9. [Connecting applications (producer / consumer)](#9-connecting-applications-producer--consumer)
10. [Kafka UI](#10-kafka-ui)
11. [Monitoring and alerting](#11-monitoring-and-alerting)
12. [Day-2 operations](#12-day-2-operations)
13. [Acceptance tests / failure drills](#13-acceptance-tests--failure-drills)
14. [DR (Disaster Recovery)](#14-dr-disaster-recovery)
15. [Lessons learned in the lab](#15-lessons-learned-in-the-lab)
16. [Requests to DevOps (checklist)](#16-requests-to-devops-checklist)
17. [Glossary](#17-glossary)

---

## 1. Big picture

| Component | Lab | Real environment |
|---|---|---|
| Servers | 3 Ubuntu VMs (Multipass + Hyper-V), 2 vCPU / 1.5 GB each | 3 separate physical servers or VMs on **different hosts/racks/AZs** |
| Kafka | 4.3.2, KRaft, combined mode (broker + controller) | Same release line; dedicated controllers if possible |
| Network | Internal Hyper-V switch, static IPs `10.10.10.11-13` | Bank server VLAN, static IPs + DNS names |
| Authentication (apps) | SASL/SCRAM-SHA-512 | SASL/SCRAM **plus TLS** (SASL_SSL) or Kerberos (AD) |
| Authorization | ACLs, deny by default | Same, one account per application (least privilege) |
| Encryption | None (SASL_PLAINTEXT) | **TLS is mandatory** |
| Kafka UI | Jar on Windows, LDAP login + RBAC | Separate server/K8s, AD (LDAPS) or SSO + RBAC |
| Human users | lldap | Active Directory |
| Passwords | `secrets.local` file (git-ignored); app passwords in `application.yml` | Vault / CyberArk, never in git |
| Monitoring | Manual (`07-verify`, Kafka UI) | Prometheus + Grafana + alerting |
| DR | None | Second DC + MirrorMaker 2 |

---

## 2. Architecture decisions

Agree on these with DevOps at the **first meeting** — every later request depends on them.

### 2.1 KRaft and controllers
- **KRaft** — Kafka 4.x has no ZooKeeper; Kafka keeps the cluster metadata itself using the Raft
  protocol. **Controllers** carry this role.
- **Controller quorum** — 3 controllers (majority = 2). Lose 1 and the cluster keeps working; lose 2
  and metadata can no longer change (no new leaders can be elected). We tested this in the lab (drill 2).
- **Combined mode** (lab): every server is both broker and controller. Possible with 3 servers, but
  Apache Kafka does **not recommend** it for critical production: broker load slows the controller down.
- **Recommendation:** 3 brokers + 3 small **dedicated controllers** (2 vCPU / 4 GB is enough).
  If DevOps says "3 servers only" — combined mode, but document the risk.

### 2.2 Replication and durability
| Setting | Value | Why |
|---|---|---|
| `default.replication.factor` | 3 | Every message has 3 copies on 3 different servers |
| `min.insync.replicas` | 2 | An `acks=all` write is not acknowledged until at least 2 copies are written |
| `unclean.leader.election.enable` | false | An out-of-sync replica never becomes leader → no data loss |
| `offsets.topic.replication.factor` | 3 | Consumer offsets must not be lost either |
| `transaction.state.log.replication.factor` / `min.isr` | 3 / 2 | Same for transactions |
| `auto.create.topics.enable` | false | No topics created by accident (typos) |

**Trade-off (CAP):** lose 1 server and writes continue; lose 2 and writes **stop**. This chooses
short unavailability over data loss — the right choice for a bank.

### 2.3 Physical placement (rack awareness)
- The 3 servers must sit on **3 different physical hosts / racks / availability zones**. If they all
  run on the same hypervisor, losing that host loses the whole cluster (in the lab all VMs were on one laptop).
- `broker.rack=<rack name>` on every broker — Kafka spreads the 3 copies of each partition across racks.
- Ask DevOps: **"Are these 3 VMs guaranteed to run on different physical hosts? Is an
  anti-affinity rule in place?"**

---

## 3. Servers (compute, disk, OS)

### 3.1 Sizing — starting point

| Resource | Broker | Dedicated controller | Note |
|---|---|---|---|
| vCPU | 8–16 | 2–4 | TLS encryption costs CPU |
| RAM | 32–64 GB | 4–8 GB | Heap is only 6 GB; the rest is Linux **page cache** |
| JVM heap | `-Xms6g -Xmx6g` | `-Xms1g -Xmx1g` | Lab used 384 MB (lab only) |
| Data disk | Dedicated disk(s), SSD/NVMe | 50–100 GB | **Must not** be the OS disk |
| OS disk | 50–100 GB | 50 GB | |
| Network | 10 GbE | 1–10 GbE | Replication traffic = writes × (RF−1) |

**Disk capacity:**

```
daily data = messages/s × average size × 86400
capacity (per broker) = daily data × retention (days) × RF / number of brokers × 1.4 (40% headroom)
```

Example: 2000 msg/s × 2 KB × 86400 ≈ 345 GB/day; 7 days, RF=3, 3 brokers →
345 × 7 × 3 / 3 × 1.4 ≈ **3.4 TB per broker**.

### 3.2 Disk
- File system: **XFS** (or ext4), mount option `noatime`.
- Several disks: `log.dirs=/data1/kafka,/data2/kafka` (JBOD) or RAID10.
- Data disk usage is monitored (alert above 75%).

### 3.3 Operating system
- RHEL 9 / Rocky 9 / Ubuntu 24.04 LTS (whatever the bank standard is).
- **Java 21** (OpenJDK, the distribution approved by the bank).
- **NTP / chrony** must run — clock skew breaks TLS, Kerberos and log analysis.
- Kernel settings (lab: `node/99-kafka.conf`):

| Setting | Value | Why |
|---|---|---|
| `vm.swappiness` | 1 | Keep the JVM out of swap |
| `vm.max_map_count` | ≥ 262144 | Kafka mmaps its index files |
| `vm.dirty_background_ratio` / `vm.dirty_ratio` | 5 / 60 | Smooth disk writes |
| `net.core.rmem_max` / `wmem_max` | 16 MB | Large socket buffers |
| `nofile` (ulimit) | ≥ 100000 | Every segment + every client = open file |

- Swap: off, or `swappiness=1`.
- Transparent Huge Pages: per bank standard (usually `madvise`).

---

## 4. Network, DNS, firewall

### 4.1 IPs and DNS
- **Static IP** for every server. In the lab a DHCP IP changed after a restart and the broker could
  not rejoin the cluster — unacceptable in a real environment.
- **DNS A** and **PTR** (reverse) records with FQDNs:
  `kafka-1.<domain>`, `kafka-2.<domain>`, `kafka-3.<domain>`.
- Kafka configuration **always uses FQDNs** (`advertised.listeners`), never IPs.
  TLS certificate SANs use the same names.

### 4.2 Ports (firewall matrix)

| Port | Protocol | From → to | Purpose |
|---|---|---|---|
| 9092 | TCP (SASL_SSL) | App servers, Kafka UI → brokers | Client traffic |
| 9092 | TCP (SASL_SSL) | Broker ↔ broker | Replication (inter-broker) |
| 9093 | TCP (SSL/mTLS) | Broker/controller ↔ controller | KRaft controller quorum |
| 7071 (or 9404) | TCP (HTTP) | Prometheus → brokers | JMX exporter metrics |
| 22 | TCP (SSH) | Admin jump host → servers | Administration |
| 636 | TCP (LDAPS) | Kafka UI → AD | UI login |
| 8090 | TCP (HTTPS) | Users → Kafka UI | Web UI (behind a reverse proxy) |

**Important:** port 9093 (controller) is open **only between the Kafka servers**, not to applications.

### 4.3 Network quality
- Latency between brokers < 1–2 ms, within one DC.
- The firewall must not drop long-idle TCP connections (idle timeout) — Kafka connections are long-lived.

---

## 5. Installation

Lab: `scripts/04-provision.ps1` + `node/install-kafka.sh`. In a real environment the same steps,
usually with **Ansible** (the bank's DevOps likely has its own Ansible/Puppet/Chef standard).

| Step | Lab | Real environment |
|---|---|---|
| Artifact | Downloaded from `archive.apache.org`, **SHA512 verified** | Bank servers often have **no internet** → Kafka tarball and Java go to an internal repo (Nexus/Artifactory), checksum verified there too |
| Service user | `kafka` system user, no login | Same (or an AD service account — per bank policy) |
| Directories | `/opt/kafka_2.13-4.3.2` + symlink `/opt/kafka`, `/etc/kafka`, `/var/lib/kafka/data`, `/var/log/kafka` | Same layout; data directory on its own disk |
| Upgrades | New directory + switch the symlink | Same — easy rollback |
| Service | systemd `kafka.service`: `Restart=on-failure`, `LimitNOFILE=100000`, `TimeoutStopSec=120` | Same |
| Config permissions | `server.properties` → `root:kafka 0640` (contains a password) | Same; passwords rendered from Vault |

**What systemd does for us:** in the lab we killed the broker with `SIGKILL`, and the OOM killer
killed it once — both times systemd restarted it within 10–13 seconds (`NRestarts`).

---

## 6. Broker configuration

Lab template: `infra/kafka-vm/node/server.properties.tmpl`. Differences for the real environment
are noted in comments.

```properties
# --- KRaft ---
process.roles=broker,controller            # dedicated nodes: controller / broker
node.id=1                                   # unique per server
controller.quorum.voters=1@kafka-1.<domain>:9093,2@kafka-2.<domain>:9093,3@kafka-3.<domain>:9093
controller.listener.names=CONTROLLER

# --- Listeners ---
listeners=SASL_SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=SASL_SSL://kafka-1.<domain>:9092
inter.broker.listener.name=SASL_SSL
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL   # lab: SASL_PLAINTEXT / PLAINTEXT

# --- Placement ---
broker.rack=rack-a

# --- Data ---
log.dirs=/data1/kafka,/data2/kafka

# --- Durability ---
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false

# --- Retention (per bank policy) ---
log.retention.hours=168

# --- Operations ---
auto.leader.rebalance.enable=true
controlled.shutdown.enable=true
```

Security settings are in the next section.

---

## 7. Security

Security has 4 layers. In the lab we fully did 2, partly 1; encryption is not done yet.

| Layer | Question | Lab | Real environment |
|---|---|---|---|
| **Encryption (in transit)** | Can traffic be read? | ❌ None | ✅ TLS (SASL_SSL, mTLS for controllers) |
| **Authentication (AuthN)** | Who is connecting? | ✅ SASL/SCRAM-SHA-512 | ✅ SCRAM or Kerberos/AD |
| **Authorization (AuthZ)** | What may they do? | ✅ ACLs, deny by default | ✅ Same, least privilege |
| **Human access (UI)** | Who may do what in the UI? | ✅ LDAP login + RBAC | ✅ AD (LDAPS) / SSO + RBAC |

### 7.1 Two separate user systems — don't mix them up

| | Human users | Kafka users (service accounts) |
|---|---|---|
| Who | Employees (admins, operators, analysts) | Applications (producer, consumer), brokers, Kafka UI |
| Where | **LDAP / Active Directory** | Kafka's own metadata (**SCRAM credentials**) or AD (Kerberos) |
| Access to | Kafka UI, Grafana, servers (SSH) | Kafka brokers (port 9092) |
| Permissions | Kafka UI **RBAC** (AD group → role) | Kafka **ACLs** (principal → resource → operation) |
| Lab examples | `admin`, `viewer` (lldap) | `broker`, `admin`, `cbar-producer`, `cbar-integration-hub`, `kafka-ui` |

In the lab the name `admin` exists in both systems — **these are two different accounts** with
different passwords. Use different names in a real environment (e.g. `kafka-ops-admin`).

### 7.2 Encryption — TLS (not done in the lab, mandatory in a real environment)

- **SASL_SSL** = SASL authentication + TLS encryption. With the lab's `SASL_PLAINTEXT`, SCRAM never
  sends the password itself, but **message contents travel in clear text** — not acceptable in a bank.
- Ask DevOps / the PKI team for:
  - A **server certificate** per broker, signed by the bank's internal CA.
    **CN / SAN** = the broker's FQDN (`kafka-1.<domain>`); no load balancer, every broker by its own name.
  - The **CA chain** for the truststore; also distributed to all applications.
  - Format: **PKCS12** (`keystore.p12`, `truststore.p12`), passwords in Vault.
  - **Certificate lifetime and rotation process** — who, when, how; alert 30 days before expiry.
- **mTLS** (both sides present certificates) for the controller listener (9093). In the lab 9093 was
  PLAINTEXT and `User:ANONYMOUS` was a super user — this **must not** exist in a real environment.
- Broker side:

```properties
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/tls/kafka-1.keystore.p12
ssl.keystore.password=<from Vault>
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/tls/truststore.p12
ssl.truststore.password=<from Vault>
ssl.endpoint.identification.algorithm=https     # clients verify the server name
ssl.enabled.protocols=TLSv1.3,TLSv1.2
listener.name.controller.ssl.client.auth=required   # mTLS for controllers
```

### 7.3 Authentication — SASL/SCRAM (done in the lab)

**How it works:** the client sends a value derived from the password, not the password itself
(challenge-response). The broker stores only a **hash** of the password (`iterations=8192`).

What we did in the lab:
1. On brokers: `sasl.enabled.mechanisms=SCRAM-SHA-512`,
   `sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512`; brokers connect to each other as `broker`.
2. On a new cluster the `broker` and `admin` SCRAM credentials are written into the metadata **at
   format time** (`kafka-storage format --add-scram ...`) — otherwise brokers cannot authenticate to
   each other on first start (chicken-and-egg).
3. Application accounts are created afterwards:

```bash
kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config admin.properties \
  --alter --entity-type users --entity-name cbar-producer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=...]'
```

4. The operator CLI runs as `admin`: `/etc/kafka/admin.properties` (readable by root only; tools via `sudo`).

**Alternatives (to discuss with DevOps/security):**

| Mechanism | When | Advantage | Difficulty |
|---|---|---|---|
| **SCRAM-SHA-512** | Simple, self-contained | No dependencies | Users managed separately inside Kafka |
| **Kerberos (SASL/GSSAPI) + AD** | Most common in banks | Accounts in AD, central management | Keytabs, KDC dependency, complex setup |
| **OAUTHBEARER + Keycloak/ADFS** | If SSO infrastructure exists | Short-lived tokens | Identity provider dependency |
| **mTLS (client certificates)** | Few apps, strong PKI | No passwords | Certificate management |

### 7.4 Authorization — ACLs (done in the lab)

- `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
- `allow.everyone.if.no.acl.found=false` — **deny by default**: no ACL, no access.
- `super.users=User:broker;User:admin` — need no ACLs.
- **ACL** = *principal* (`User:cbar-producer`) + *resource* (topic / group / cluster /
  transactional-id) + *operation* (Read / Write / Describe / Create / ...) + *permission* (Allow/Deny).

Lab ACLs (`infra/kafka-vm/node/kafka-security.sh`):

| Principal | Resource | Operations |
|---|---|---|
| `cbar-producer` | topic `*` | Write, Describe, Create, DescribeConfigs |
| `cbar-integration-hub` | topic `*` | Read, Describe |
| `cbar-integration-hub` | group `*` | Read |
| `kafka-ui` | cluster, topic `*`, group `*`, transactional-id `*` | All |

**Real-environment difference — least privilege:** in the lab the accounts can access every topic
(for simplicity). In a real environment, **one account per application**, limited to its own topics / group:

```bash
# producer: writes only to its own topic
kafka-acls.sh ... --add --allow-principal User:payments-producer \
  --topic payments.v1 --operation Write --operation Describe
# consumer: only its own topic and its own group
kafka-acls.sh ... --add --allow-principal User:payments-consumer \
  --topic payments.v1 --operation Read --operation Describe
kafka-acls.sh ... --add --allow-principal User:payments-consumer \
  --group payments-consumer --operation Read
# several topics by prefix: --resource-pattern-type prefixed --topic payments.
```

Verified in the lab: a client without credentials gets nothing; a wrong password →
`Authentication failed`; the producer account cannot read, the consumer account cannot write or
create topics → `AuthorizationException`.

**Whose job is creating topics?** In the lab the producer app creates its topic in code (it has
`Create`). In a real environment topics are usually created by the **platform team** (ticket + IaC)
and applications do not get `Create`.

### 7.5 Passwords / secrets

| Lab | Real environment |
|---|---|
| `infra/kafka-vm/secrets.local`, `infra/ldap/secrets.local` (git-ignored, randomly generated) | **HashiCorp Vault / CyberArk** |
| App passwords in plain text in `application.yml` (lab decision; the repo is public) | **Never** in git: env vars, Vault agent, K8s Secret |
| No password rotation | Rotation process: new password → hand to the app → remove the old one |

### 7.6 Kafka UI access — LDAP + RBAC (done in the lab)

- **LDAP** — the protocol for talking to a user directory. In the bank this is **Active Directory**.
- Login: Kafka UI **binds** to LDAP with the user's name and password (it never stores the password).
- Groups are read from LDAP with a separate **read-only service account** (`kafka-ui-bind` in the lab).
- **RBAC**: AD group → Kafka UI role.

| AD group (lab) | Role | Permissions |
|---|---|---|
| `kafka-admins` | admins | Everything |
| `kafka-viewers` | viewers | See brokers/topics/partitions/lag; **not message contents** |

Messages can contain personal and financial data — grant `MESSAGES_READ` only to those who need it.

Ask DevOps / the AD team for:
- AD groups: `kafka-admins`, `kafka-viewers` (names per bank standard).
- A **service account** (bind user) for Kafka UI, read-only, password in Vault.
- The **LDAPS** (636) address and the AD CA certificate.
- User search base / group search base (e.g. `OU=Users,DC=cbar,DC=az`).

### 7.7 Audit
- Who changed which ACL and when, who deleted a topic — Kafka's authorizer log
  (`kafka-authorizer.log`) must be shipped to the central log system (ELK/Splunk/SIEM).
- Kafka UI's audit feature can be enabled as well.

---

## 8. First start of the cluster (bootstrap)

Lab: `scripts/05-bootstrap-cluster.ps1` + `node/format-storage.sh`.

1. Generate the **cluster ID** once: `kafka-storage.sh random-uuid`.
2. Format storage on every server **with the same cluster ID**, including the super users' SCRAM credentials:
   ```bash
   kafka-storage.sh format --config server.properties --cluster-id <ID> \
     --add-scram 'SCRAM-SHA-512=[name=broker,password=...]' \
     --add-scram 'SCRAM-SHA-512=[name=admin,password=...]'
   ```
   Formatting is a **one-time** operation; it is never repeated on a running cluster (`--ignore-formatted`).
3. Start all 3 servers **at the same time** (so the quorum can elect a leader).
4. Create application accounts and ACLs (`06-security.ps1`).
5. Verify (`07-verify.ps1`): service state, quorum (leader + 3 voters, lag 0), test topic RF=3,
   under-replicated partitions = 0.

**Moving an existing cluster from PLAINTEXT to SASL** (lab: `migrate-to-sasl.ps1`) — in the lab we
did it with a short outage. In a real environment, the **zero-downtime** path, one rolling restart per phase:
1. Add a SASL listener next to PLAINTEXT.
2. Switch `inter.broker.listener.name` to SASL.
3. Move the applications, enable the authorizer, remove PLAINTEXT.

---

## 9. Connecting applications (producer / consumer)

Lab: `kafka-producer` (REST → Kafka) and `cbar-integration-hub` (`@KafkaListener`), Spring Boot 4.1.

### 9.1 Connection settings (real environment)

```yaml
spring:
  kafka:
    bootstrap-servers: kafka-1.<domain>:9092,kafka-2.<domain>:9092,kafka-3.<domain>:9092
    security:
      protocol: SASL_SSL                     # lab: SASL_PLAINTEXT
    ssl:
      trust-store-location: file:/etc/app/truststore.p12
      trust-store-password: ${TRUSTSTORE_PASSWORD}
      trust-store-type: PKCS12
    properties:
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
```

### 9.2 Producer — so that no message is lost
| Setting | Value | Why |
|---|---|---|
| `acks` | all | All in-sync replicas must write |
| `enable.idempotence` | true | No duplicates on retry |
| `delivery.timeout.ms` | 30000 | Not acknowledged within this time → error |
| `max.block.ms` | 10000 | Limit for waiting on metadata |
| `send()` result | **always checked** | In the lab the console producer swallowed the error and 1 message was lost |

### 9.3 Consumer — a message is not "read" before it is processed
| Setting | Value | Why |
|---|---|---|
| `enable-auto-commit` | false | Offsets are committed after processing, not on a timer |
| `ack-mode` | record | Commit after each message (at-least-once) |
| `concurrency` | = number of partitions | One thread per partition |
| Error handler | retry + DLT | One bad message must not block its partition |

**At-least-once** = no message is lost, but rarely one can arrive **twice** → the consumer must be
**idempotent** (e.g. check duplicates by payment ID).

### 9.4 Keys and partitions
- Same key → same partition → **order is kept** for that key (e.g. with the account number as key,
  one account's transactions are processed in sequence).
- The number of partitions can only **grow**, never shrink; growing it changes the key → partition
  mapping. Plan it upfront.

---

## 10. Kafka UI

Lab: Kafbat Kafka UI 1.5.0, jar on Windows, `127.0.0.1:8090`.

| | Lab | Real environment |
|---|---|---|
| Where | Developer laptop | Separate VM or Kubernetes |
| Access | `127.0.0.1` | HTTPS reverse proxy (nginx), internal network |
| Login | lldap (LDAP) | AD (LDAPS) or SSO (OAuth2) |
| Roles | admins, viewers | Maybe also `operators` (offset reset, but no topic deletion) |
| Connection to Kafka | `kafka-ui` SCRAM account | Same, over SASL_SSL |
| `readOnly` | false (permissions come from RBAC) | Often `true` on the production cluster |

---

## 11. Monitoring and alerting

Built in the lab: `infra/monitoring` (Prometheus + Grafana on Windows, JMX exporter and node_exporter
on the VMs, Spring Boot apps via `/actuator/prometheus`). Stopping a broker fired `KafkaBrokerDown`
and the URP alert, which cleared after recovery. In a real environment monitoring must be ready
**before go-live**, plus Alertmanager for notifications.

**Stack:** JMX exporter (Java agent on every broker) → **Prometheus** → **Grafana**; alerts via
Alertmanager → e-mail / Teams / PagerDuty. Logs → ELK / Splunk.

| Metric | Normal | Alert |
|---|---|---|
| `UnderReplicatedPartitions` | 0 | > 0 for more than 5 minutes |
| `OfflinePartitionsCount` | 0 | > 0 — **critical** |
| `UnderMinIsrPartitionCount` | 0 | > 0 — writes are blocked |
| `ActiveControllerCount` (sum) | 1 | ≠ 1 |
| ISR shrink / expand rate | ~0 | Frequent changes — network/disk problem |
| Consumer lag | Stable / low | Keeps growing |
| Request latency (Produce p99) | ms | Sharp increase |
| Disk usage | < 70% | > 75% |
| JVM GC pauses, heap | — | Long pauses |
| Broker process / `NRestarts` | — | It restarted (OOM?) |
| TLS certificate expiry | — | < 30 days |

---

## 12. Day-2 operations

### 12.1 Rolling restart (config change, patching, certificate renewal)
**One server** at a time:
1. `systemctl restart kafka` (controlled shutdown hands leadership over).
2. Wait: `UnderReplicatedPartitions = 0`, ISR complete.
3. Then the next server.

In the lab we grew the VMs from 1 GB to 1.5 GB exactly this way — no data lost, ISR recovered in
40–63 seconds each time.

### 12.2 Upgrade
Unpack the new version into its own directory → check the configuration → switch the symlink →
rolling restart → once all brokers are stable on the new version, bump `metadata.version`.
Always read the release notes.

### 12.3 Stopping the whole cluster (planned)
Stop the brokers **in parallel**. Stopped one by one, the last ones wait for the quorum, hang until
`TimeoutStopSec` (120 s) and then get `SIGKILL` — this happened in the lab.

### 12.4 Preferred leader election
After a server returns, leaders stay unbalanced. `auto.leader.rebalance.enable=true` fixes it within
≤5 minutes, or immediately:
`kafka-leader-election.sh --election-type PREFERRED --all-topic-partitions`.

### 12.5 New application / topic request (process)
The application team requests via ticket: topic name, partition count, retention, expected volume,
producer/consumer accounts → the platform team creates it (IaC, review) → ACLs → passwords to Vault →
the application team is notified.

---

## 13. Acceptance tests / failure drills

Run these in the real environment before go-live (lab: `scripts/08-drills.ps1`):

| # | Test | Expected | Lab result |
|---|---|---|---|
| 1 | Stop 1 server | ISR 3→2, writes continue | 500/500 ✅ |
| 2 | Stop 2 servers | Writes stop (`NOT_ENOUGH_REPLICAS`), no data loss | 0/300 ✅ |
| 3 | Servers come back | ISR 3, no acknowledged message lost | 500/500 ✅ |
| 4 | `kill -9` the broker process | systemd restarts it, writes continue | 300/300 ✅ |
| 5 | No account / wrong password | Cannot connect | ✅ |
| 6 | App performs an operation it has no permission for | `AuthorizationException` | ✅ |
| 7 | Rolling restart | No data loss | ✅ |
| 8 | Disk full, network partition | Alert fires | Real environment |
| 9 | DR failover | Within RPO/RTO | Real environment |

---

## 14. DR (Disaster Recovery)

- One cluster survives losing a server **within one DC**, but not losing **the whole DC**.
- A separate cluster in a **second DC** + replication with **MirrorMaker 2** (MM2):
  topics, consumer offsets (translation) and ACLs.
- Numbers to agree on (may be a regulatory requirement):
  - **RPO** (Recovery Point Objective) — how much data loss is acceptable (MM2 is asynchronous → seconds).
  - **RTO** (Recovery Time Objective) — how fast service must be restored.
- Failover and failback must be written down and **tested**.
- Instead of "backing up" Kafka, the usual approach is **replication + a DR cluster**.

---

## 15. Lessons learned in the lab

| What happened | Result | Lesson for the real environment |
|---|---|---|
| A broker VM restarted, DHCP gave it a new IP | The broker could not rejoin the cluster | Static IPs + DNS |
| Broker + CLI JVM on a 1 GB VM | OOM killer killed the broker | Size RAM properly; watch extra JVMs on the server |
| Brokers stopped one after another | Each hung for 120 s, then `SIGKILL` | Full stop in parallel; rolling restart one by one |
| Producer's first request went to a dead broker | 1 message lost, the console producer swallowed the error | Always check the `send()` callback; alert / retry / DLT |
| `acks=all` timed out, but the message showed up later | Retry → risk of duplicates | Idempotent producer + idempotent consumer |
| Quorum was lost | The producer did not fail, it hung | Timeouts / circuit breaker in the app; monitoring |
| `netplan apply` | Dropped the SSH session | Apply network changes only to the interface concerned |
| Password in a config file | `0640 root:kafka` | Files with secrets get the narrowest permissions |

---

## 16. Requests to DevOps (checklist)

### Infrastructure / compute
- [ ] 3 broker servers (or 3 brokers + 3 controllers): vCPU, RAM, disk — section 3.1
- [ ] Servers on **different physical hosts / racks / AZs** (anti-affinity) — rack names
- [ ] Dedicated data disk(s), SSD, XFS, `noatime`; capacity calculation attached
- [ ] OS: \<bank standard\>, Java 21, NTP/chrony
- [ ] Kernel settings and `nofile` limit — section 3.3
- [ ] `kafka` service user; `sudo` rules for admins

### Network
- [ ] Static IPs; DNS A + PTR records (FQDN)
- [ ] Firewall rules — section 4.2 (port matrix)
- [ ] Check broker-to-broker latency and idle timeouts

### Artifacts
- [ ] Kafka tarball (version) + SHA512 in the internal repo (Nexus/Artifactory)
- [ ] JMX exporter jar in the internal repo

### Security
- [ ] TLS certificate per broker (CN/SAN = FQDN), CA chain, PKCS12
- [ ] Certificate renewal process and expiry alert
- [ ] AuthN mechanism decision: SCRAM / Kerberos / OAuth
- [ ] Super-user accounts (`broker`, operator admin) — passwords in Vault
- [ ] One account + ACLs per application (least privilege); passwords in Vault
- [ ] AD groups (`kafka-admins`, `kafka-viewers`), LDAP bind service account for Kafka UI, LDAPS
- [ ] Authorizer / audit logs shipped to the SIEM

### Monitoring and logs
- [ ] Prometheus scrape (JMX exporter port), Grafana dashboard, alert rules — section 11
- [ ] Logs shipped to the central system (`/var/log/kafka`)

### Process and documentation
- [ ] Ticket template and approval flow for topic / ACL requests
- [ ] Runbooks: rolling restart, upgrade, certificate renewal
- [ ] Acceptance tests (section 13) — who runs them and when
- [ ] DR: second DC, MM2, RPO/RTO, failover test
- [ ] On-call / escalation: who receives alerts

---

## 17. Glossary

| Term | Meaning |
|---|---|
| **Broker** | Kafka server that stores and serves messages |
| **Controller** | Role that manages cluster metadata (leader election, topics, ACLs) |
| **KRaft** | Kafka's ZooKeeper-less metadata mode (Raft protocol) |
| **Quorum** | Majority needed for a decision; 2 of 3 controllers |
| **Combined mode** | One server acts as both broker and controller |
| **Topic** | Logical channel of messages (e.g. `cbar.messages`) |
| **Partition** | Part of a topic; the unit of parallelism and ordering |
| **Offset** | Position of a message within a partition |
| **Replica / RF** | Copy of a partition / number of copies (Replication Factor) |
| **Leader / Follower** | The replica that takes writes / the replicas copying it |
| **ISR** | In-Sync Replicas — replicas caught up with the leader |
| **URP** | Under-Replicated Partitions — partitions whose ISR is not complete |
| **min.insync.replicas** | Minimum ISR size for an `acks=all` write |
| **acks** | Producer acknowledgement: 0 / 1 / all |
| **Idempotent producer** | Producer that does not create duplicates on retry |
| **Consumer group** | Consumers sharing a topic's partitions among themselves |
| **Consumer lag** | How far a consumer is behind the latest message |
| **Rebalance** | Redistributing partitions within a consumer group |
| **At-least-once** | No message is lost, but one can arrive twice |
| **DLT / DLQ** | Dead Letter Topic/Queue — for messages that cannot be processed |
| **Retention** | How long messages are kept |
| **broker.rack / rack awareness** | Spreading replicas across racks |
| **Unclean leader election** | An out-of-sync replica becoming leader (risk of data loss) |
| **Controlled shutdown** | A broker hands over leadership before stopping |
| **Rolling restart** | Restarting servers one at a time |
| **Preferred leader** | A partition's first replica; should be leader for balance |
| **Listener** | Address + protocol a broker listens on (`SASL_SSL://:9092`) |
| **advertised.listeners** | The address a broker tells clients to connect to |
| **SASL** | Authentication framework (SCRAM, GSSAPI, PLAIN, OAUTHBEARER) |
| **SCRAM-SHA-512** | Challenge-response authentication without sending the password |
| **SASL_PLAINTEXT / SASL_SSL** | SASL without encryption / SASL + TLS |
| **TLS / mTLS** | Encryption / both sides authenticate with certificates |
| **Keystore / Truststore** | Own certificate + key / trusted CAs |
| **CN / SAN** | Name in a certificate / alternative names (FQDNs) |
| **Principal** | Authenticated identity in Kafka (`User:cbar-producer`) |
| **ACL** | Rule: principal + resource + operation + permission |
| **Authorizer** | Component that checks ACLs (`StandardAuthorizer`) |
| **Super user** | Account that bypasses ACL checks |
| **Deny by default** | No ACL means no permission |
| **Least privilege** | Every account gets only the minimum it needs |
| **LDAP / AD** | User directory protocol / Microsoft's directory server |
| **Bind** | Authenticating to LDAP with a username + password |
| **RBAC** | Role-based access control (group → role → permissions) |
| **Service account** | Account for an application, not a person |
| **Vault** | Central store for secrets (passwords, keys) |
| **JMX exporter** | Agent exposing Kafka metrics in Prometheus format |
| **MirrorMaker 2** | Cross-cluster replication tool (for DR) |
| **RPO / RTO** | Acceptable data loss / recovery time |
| **IaC** | Infrastructure as Code — infrastructure in code, with review |
| **Runbook** | Step-by-step instructions for an operation |
