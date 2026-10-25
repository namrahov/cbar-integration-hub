# Kafka klasterinin real mühitdə (3 server) qurulması — DevOps üçün təlimat

> English version: [PRODUCTION-SETUP.en.md](PRODUCTION-SETUP.en.md)

Bu sənəd lab-da (bir laptopda, 3 VM üzərində) etdiklərimizi addım-addım izah edir və hər
addımın **real mühitdə nəyə çevrildiyini**, **DevOps / infrastruktur / şəbəkə / təhlükəsizlik
komandalarından nə istəməli olduğunu** göstərir. Terminlər ingiliscə saxlanılıb, çünki tiketlərdə,
sənədlərdə və müsahibələrdə məhz bu adlarla işlədilir.

> Lab-ın kodu: `infra/kafka-vm` (klaster), `infra/kafka-ui` (UI), `infra/ldap` (LDAP),
> `src/` (consumer), ayrıca repo `kafka-producer` (producer).

---

## Mündəricat

1. [Ümumi mənzərə](#1-ümumi-mənzərə)
2. [Arxitektura qərarları](#2-arxitektura-qərarları)
3. [Serverlər (compute, disk, OS)](#3-serverlər-compute-disk-os)
4. [Şəbəkə, DNS, firewall](#4-şəbəkə-dns-firewall)
5. [Quraşdırma (install)](#5-quraşdırma-install)
6. [Broker konfiqurasiyası](#6-broker-konfiqurasiyası)
7. [Təhlükəsizlik (security)](#7-təhlükəsizlik-security)
8. [Klasterin ilk dəfə işə salınması (bootstrap)](#8-klasterin-ilk-dəfə-işə-salınması-bootstrap)
9. [Tətbiqlərin qoşulması (producer / consumer)](#9-tətbiqlərin-qoşulması-producer--consumer)
10. [Kafka UI](#10-kafka-ui)
11. [Monitorinq və alerting](#11-monitorinq-və-alerting)
12. [Gündəlik əməliyyatlar (Day-2 operations)](#12-gündəlik-əməliyyatlar-day-2-operations)
13. [Qəbul testləri (acceptance / failure drills)](#13-qəbul-testləri-acceptance--failure-drills)
14. [DR (Disaster Recovery)](#14-dr-disaster-recovery)
15. [Lab-da öyrəndiyimiz dərslər](#15-lab-da-öyrəndiyimiz-dərslər)
16. [DevOps-a göndəriləcək sorğular (checklist)](#16-devops-a-göndəriləcək-sorğular-checklist)
17. [Terminlər lüğəti](#17-terminlər-lüğəti)

---

## 1. Ümumi mənzərə

| Komponent | Lab-da | Real mühitdə |
|---|---|---|
| Serverlər | 3 Ubuntu VM (Multipass + Hyper-V), hər biri 2 vCPU / 1.5 GB | 3 ayrı fiziki server və ya VM, **fərqli host/rack/AZ**-da |
| Kafka | 4.3.2, KRaft, combined mode (broker + controller) | Eyni versiya xətti; mümkünsə ayrıca controller-lər |
| Şəbəkə | Daxili Hyper-V switch, statik IP `10.10.10.11-13` | Bankın server VLAN-ı, statik IP + DNS adları |
| Autentifikasiya (tətbiqlər) | SASL/SCRAM-SHA-512 | SASL/SCRAM **üstəgəl TLS** (SASL_SSL) və ya Kerberos (AD) |
| Avtorizasiya | ACL-lər, deny-by-default | Eyni, hər tətbiqə ayrıca hesab (least privilege) |
| Şifrələmə | Yoxdur (SASL_PLAINTEXT) | **TLS mütləqdir** |
| Kafka UI | Windows-da jar, LDAP login + RBAC | Ayrıca server/K8s, AD (LDAPS) və ya SSO + RBAC |
| İnsan istifadəçiləri | lldap | Active Directory |
| Parollar | `secrets.local` faylı (git-ignored); app parolları `application.yml`-da | Vault / CyberArk, heç vaxt git-də deyil |
| Monitorinq | Əl ilə (`07-verify`, Kafka UI) | Prometheus + Grafana + alerting |
| DR | Yoxdur | İkinci DC + MirrorMaker 2 |

---

## 2. Arxitektura qərarları

Bu qərarlar DevOps ilə **ilk görüşdə** razılaşdırılmalıdır, çünki hamısı sonrakı sorğulara təsir edir.

### 2.1 KRaft və controller-lər
- **KRaft** — Kafka 4.x-də ZooKeeper yoxdur; klasterin metadata-sını Kafka özü, Raft protokolu ilə
  saxlayır. Bu rolu **controller**-lər daşıyır.
- **Controller quorum** — 3 controller (majority = 2). 1 controller itsə klaster işləyir, 2 itsə
  metadata dəyişə bilməz (yeni leader seçilə bilməz). Lab-da bunu sınadıq (drill 2).
- **Combined mode** (lab-da): hər server həm broker, həm controller. 3 serverlə mümkündür, amma
  Apache Kafka bunu kritik production üçün **tövsiyə etmir**: brokerin yükü controller-i yavaşladır.
- **Tövsiyə:** 3 broker + 3 kiçik **dedicated controller** (2 vCPU / 4 GB kifayətdir).
  DevOps "cəmi 3 server" deyirsə — combined mode, amma riski sənədləşdir.

### 2.2 Replikasiya və davamlılıq (durability)
| Parametr | Dəyər | Niyə |
|---|---|---|
| `default.replication.factor` | 3 | Hər mesajın 3 surəti, 3 fərqli serverdə |
| `min.insync.replicas` | 2 | `acks=all` yazışı ən azı 2 surət yazılmadan təsdiqlənmir |
| `unclean.leader.election.enable` | false | Geridə qalmış replika leader olmur → data itmir |
| `offsets.topic.replication.factor` | 3 | Consumer offset-ləri də itməməlidir |
| `transaction.state.log.replication.factor` / `min.isr` | 3 / 2 | Transaction-lar üçün eyni |
| `auto.create.topics.enable` | false | Topic-lər səhvən (typo ilə) yaranmasın |

**Trade-off (CAP):** 1 server itsə yazma davam edir; 2 server itsə yazma **dayanır**. Bu,
data itkisindənsə qısa müddətli əlçatmazlığı seçməkdir — bank üçün düzgün seçim.

### 2.3 Fiziki yerləşmə (rack awareness)
- 3 server **3 fərqli fiziki host / rack / availability zone**-da olmalıdır. Hamısı eyni
  hypervisor-da olsa, o host gedəndə bütün klaster gedir (lab-da bütün VM-lər bir laptopda idi).
- Hər brokerdə `broker.rack=<rack adı>` — Kafka hər partition-un 3 surətini fərqli rack-lara
  paylayır.
- DevOps-dan soruş: **"Bu 3 VM-in fərqli fiziki host-larda olmasına zəmanət var? Anti-affinity
  qaydası qoyulubmu?"**

---

## 3. Serverlər (compute, disk, OS)

### 3.1 Ölçülər (sizing) — başlanğıc tövsiyəsi

| Resurs | Broker | Dedicated controller | Qeyd |
|---|---|---|---|
| vCPU | 8–16 | 2–4 | TLS şifrələmə CPU yeyir |
| RAM | 32–64 GB | 4–8 GB | Heap cəmi 6 GB; qalanı Linux **page cache** üçündür |
| JVM heap | `-Xms6g -Xmx6g` | `-Xms1g -Xmx1g` | Lab-da 384 MB idi (yalnız lab üçün) |
| Data diski | Ayrıca disk(lər), SSD/NVMe | 50–100 GB | OS diski ilə **eyni olmamalıdır** |
| OS diski | 50–100 GB | 50 GB | |
| Şəbəkə | 10 GbE | 1–10 GbE | Replikasiya trafiki = yazma × (RF−1) |

**Disk tutumunun hesablanması:**

```
gündəlik data = mesaj/san × orta ölçü × 86400
tutum (bir broker) = gündəlik data × retention (gün) × RF / broker sayı × 1.4 (40% ehtiyat)
```

Məsələn: 2000 mesaj/san × 2 KB × 86400 ≈ 345 GB/gün; 7 gün, RF=3, 3 broker →
345 × 7 × 3 / 3 × 1.4 ≈ **3.4 TB hər brokerə**.

### 3.2 Disk
- Fayl sistemi: **XFS** (və ya ext4), mount seçimi `noatime`.
- Bir neçə disk varsa: `log.dirs=/data1/kafka,/data2/kafka` (JBOD) və ya RAID10.
- Data diski monitorinqə daxil olmalıdır (doluluq > 75% alert).

### 3.3 Əməliyyat sistemi
- RHEL 9 / Rocky 9 / Ubuntu 24.04 LTS (bankın standartı nədirsə).
- **Java 21** (OpenJDK, bankın təsdiqlədiyi distributiv).
- **NTP / chrony** mütləq işləməlidir — saat fərqi TLS, Kerberos və log analizini pozur.
- Kernel tənzimləmələri (lab-da `node/99-kafka.conf`):

| Parametr | Dəyər | Niyə |
|---|---|---|
| `vm.swappiness` | 1 | JVM swap-a düşməsin |
| `vm.max_map_count` | ≥ 262144 | Kafka index fayllarını mmap edir |
| `vm.dirty_background_ratio` / `vm.dirty_ratio` | 5 / 60 | Diskə yazma hamar getsin |
| `net.core.rmem_max` / `wmem_max` | 16 MB | Böyük socket buffer-lər |
| `nofile` (ulimit) | ≥ 100000 | Hər segment + hər client = açıq fayl |

- Swap: söndürülmüş və ya `swappiness=1`.
- Transparent Huge Pages: bankın standartına uyğun (adətən `madvise`).

---

## 4. Şəbəkə, DNS, firewall

### 4.1 IP və DNS
- **Statik IP** hər serverə. Lab-da DHCP IP restart-dan sonra dəyişdi və broker klasterə
  qayıda bilmədi — real mühitdə bu qəbuledilməzdir.
- **DNS A** və **PTR** (reverse) qeydləri, FQDN ilə:
  `kafka-1.<domain>`, `kafka-2.<domain>`, `kafka-3.<domain>`.
- Kafka konfiqurasiyasında **həmişə FQDN** istifadə olunur (`advertised.listeners`), IP yox.
  TLS sertifikatlarının SAN-ı da bu adlarla olacaq.

### 4.2 Portlar (firewall matrisi)

| Port | Protokol | Haradan → haraya | Nə üçün |
|---|---|---|---|
| 9092 | TCP (SASL_SSL) | Tətbiq serverləri, Kafka UI → brokerlər | Client trafiki |
| 9092 | TCP (SASL_SSL) | Broker ↔ broker | Replikasiya (inter-broker) |
| 9093 | TCP (SSL/mTLS) | Broker/controller ↔ controller | KRaft controller quorum |
| 7071 (və ya 9404) | TCP (HTTP) | Prometheus → brokerlər | JMX exporter metrikləri |
| 22 | TCP (SSH) | Admin jump host → serverlər | Administrasiya |
| 636 | TCP (LDAPS) | Kafka UI → AD | UI login |
| 8090 | TCP (HTTPS) | İstifadəçilər → Kafka UI | Web interfeys (reverse proxy arxasında) |

**Vacib:** 9093 (controller) portu **yalnız Kafka serverləri arasında** açıq olmalıdır, tətbiqlərə yox.

### 4.3 Şəbəkə keyfiyyəti
- Brokerlər arası gecikmə (latency) < 1–2 ms, eyni DC daxilində.
- Firewall-un uzun müddət boş qalan TCP bağlantılarını kəsməməsi (idle timeout) — Kafka
  bağlantıları uzunömürlüdür.

---

## 5. Quraşdırma (install)

Lab-da `scripts/04-provision.ps1` + `node/install-kafka.sh`. Real mühitdə eyni addımlar,
adətən **Ansible** ilə (bankda çox güman ki, DevOps-un öz Ansible/Puppet/Chef standartı var).

| Addım | Lab-da necə | Real mühitdə |
|---|---|---|
| Artifact | `archive.apache.org`-dan endirilir, **SHA512 yoxlanılır** | Bank serverlərində çox vaxt **internet yoxdur** → Kafka tarball-ı və Java daxili repoya (Nexus/Artifactory) yüklənir, checksum orada da yoxlanılır |
| Service user | `kafka` sistem istifadəçisi, login yoxdur | Eyni (və ya AD-dən gələn servis hesabı — bankın siyasətinə görə) |
| Qovluqlar | `/opt/kafka_2.13-4.3.2` + symlink `/opt/kafka`, `/etc/kafka`, `/var/lib/kafka/data`, `/var/log/kafka` | Eyni struktur; data qovluğu ayrıca diskdə |
| Versiya yeniləmə | Yeni qovluq + symlink dəyişmək | Eyni — rollback asandır |
| Servis | systemd `kafka.service`: `Restart=on-failure`, `LimitNOFILE=100000`, `TimeoutStopSec=120` | Eyni |
| Konfiqurasiya icazələri | `server.properties` → `root:kafka 0640` (parol var) | Eyni; parollar Vault-dan render olunur |

**systemd-nin rolu:** lab-da broker prosesini `SIGKILL` ilə öldürdük və OOM killer onu öldürdü —
hər iki halda systemd 10–13 saniyəyə özü yenidən başlatdı (`NRestarts`).

---

## 6. Broker konfiqurasiyası

Lab-ın şablonu: `infra/kafka-vm/node/server.properties.tmpl`. Real mühit üçün əsas fərqlər
**qalın** yazılıb.

```properties
# --- KRaft ---
process.roles=broker,controller            # dedicated controller-lərdə: controller / broker
node.id=1                                   # hər serverdə unikal
controller.quorum.voters=1@kafka-1.<domain>:9093,2@kafka-2.<domain>:9093,3@kafka-3.<domain>:9093
controller.listener.names=CONTROLLER

# --- Listeners ---
listeners=SASL_SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=SASL_SSL://kafka-1.<domain>:9092
inter.broker.listener.name=SASL_SSL
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL   # lab-da: SASL_PLAINTEXT / PLAINTEXT

# --- Yerləşmə ---
broker.rack=rack-a

# --- Data ---
log.dirs=/data1/kafka,/data2/kafka

# --- Davamlılıq ---
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false

# --- Retention (bank siyasətinə görə) ---
log.retention.hours=168

# --- Əməliyyat ---
auto.leader.rebalance.enable=true
controlled.shutdown.enable=true
```

Təhlükəsizlik parametrləri növbəti bölmədədir.

---

## 7. Təhlükəsizlik (security)

Təhlükəsizlik 4 qatdan ibarətdir. Lab-da 2-sini tam, 1-ni qismən etdik; şifrələmə hələ yoxdur.

| Qat | Sual | Lab-da | Real mühitdə |
|---|---|---|---|
| **Encryption (in transit)** | Trafik oxuna bilərmi? | ❌ Yoxdur | ✅ TLS (SASL_SSL, controller üçün mTLS) |
| **Authentication (AuthN)** | Qoşulan kimdir? | ✅ SASL/SCRAM-SHA-512 | ✅ SCRAM və ya Kerberos/AD |
| **Authorization (AuthZ)** | Ona nə icazə var? | ✅ ACL-lər, deny-by-default | ✅ Eyni, least privilege |
| **İnsanların girişi (UI)** | Kim UI-da nə edə bilər? | ✅ LDAP login + RBAC | ✅ AD (LDAPS) / SSO + RBAC |

### 7.1 İki ayrı istifadəçi sistemi — çaşdırmamaq üçün

| | İnsan istifadəçiləri | Kafka user-ləri (servis hesabları) |
|---|---|---|
| Kim | Əməkdaşlar (admin, operator, analitik) | Tətbiqlər (producer, consumer), broker, Kafka UI |
| Harada | **LDAP / Active Directory** | Kafka-nın öz metadata-sında (**SCRAM credentials**) və ya AD (Kerberos) |
| Nəyə girir | Kafka UI, Grafana, serverlər (SSH) | Kafka brokerləri (port 9092) |
| İcazə | Kafka UI **RBAC** (AD qrupu → rol) | Kafka **ACL** (principal → resurs → əməliyyat) |
| Lab-da nümunə | `admin`, `viewer` (lldap) | `broker`, `admin`, `cbar-producer`, `cbar-integration-hub`, `kafka-ui` |

Lab-da `admin` adı hər iki sistemdə var — **bu, iki fərqli hesabdır**, parolları da fərqlidir.
Real mühitdə fərqli ad ver (məsələn `kafka-ops-admin`).

### 7.2 Şifrələmə — TLS (lab-da edilməyib, real mühitdə mütləqdir)

- **SASL_SSL** = SASL autentifikasiyası + TLS şifrələmə. Lab-dakı `SASL_PLAINTEXT`-də SCRAM parolu
  açıq göndərmir, amma **mesajların məzmunu açıq gedir** — bankda qəbuledilməzdir.
- DevOps / PKI komandasından istəniləcək:
  - Hər broker üçün **server sertifikatı**, bankın daxili CA-sı ilə imzalanmış.
    **CN / SAN** = brokerin FQDN-i (`kafka-1.<domain>`); load balancer yoxdur, hər broker öz adı ilə.
  - **CA zənciri (chain)** — truststore üçün; bütün tətbiqlərə də paylanır.
  - Format: **PKCS12** (`keystore.p12`, `truststore.p12`), parolları Vault-da.
  - **Sertifikat müddəti və yeniləmə (rotation) prosesi** — kim, nə vaxt, necə; müddət bitməzdən
    30 gün əvvəl alert.
- Controller listener (9093) üçün **mTLS** (hər iki tərəf sertifikat göstərir). Lab-da 9093
  PLAINTEXT idi və `User:ANONYMOUS` super user edilmişdi — real mühitdə bu **olmamalıdır**.
- Broker tərəfi:

```properties
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/tls/kafka-1.keystore.p12
ssl.keystore.password=<Vault-dan>
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/tls/truststore.p12
ssl.truststore.password=<Vault-dan>
ssl.endpoint.identification.algorithm=https     # client server adını yoxlasın
ssl.enabled.protocols=TLSv1.3,TLSv1.2
listener.name.controller.ssl.client.auth=required   # controller üçün mTLS
```

### 7.3 Autentifikasiya — SASL/SCRAM (lab-da etdik)

**Necə işləyir:** client parolun özünü yox, ondan hesablanmış cavabı göndərir
(challenge-response). Broker parolu yox, yalnız **hash**-ini saxlayır (`iterations=8192`).

Lab-da edilən:
1. Brokerlərdə: `sasl.enabled.mechanisms=SCRAM-SHA-512`,
   `sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512`; brokerlər bir-birinə `broker` hesabı ilə
   qoşulur.
2. Yeni klasterdə `broker` və `admin` SCRAM məlumatı **format zamanı** metadata-ya yazılır
   (`kafka-storage format --add-scram ...`) — əks halda brokerlər ilk dəfə bir-birinə qoşula bilməz
   ("toyuq-yumurta" problemi).
3. Tətbiq hesabları sonra yaradılır:

```bash
kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config admin.properties \
  --alter --entity-type users --entity-name cbar-producer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=...]'
```

4. Operatorun CLI-ı `admin` hesabı ilə işləyir: `/etc/kafka/admin.properties` (yalnız root oxuyur,
   alətlər `sudo` ilə).

**Alternativlər (DevOps/security ilə müzakirə):**

| Mexanizm | Nə vaxt | Üstünlük | Çətinlik |
|---|---|---|---|
| **SCRAM-SHA-512** | Sadə, müstəqil | Asılılıq yoxdur | User-lər Kafka daxilində ayrıca idarə olunur |
| **Kerberos (SASL/GSSAPI) + AD** | Banklarda ən çox yayılmış | Hesablar AD-də, mərkəzi idarə | Keytab-lar, KDC asılılığı, mürəkkəb qurulum |
| **OAUTHBEARER + Keycloak/ADFS** | SSO infrastrukturu varsa | Token, qısa ömürlü | Identity provider asılılığı |
| **mTLS (client sertifikatı)** | Tətbiq sayı az, PKI güclüdür | Parol yoxdur | Sertifikatların idarəsi |

### 7.4 Avtorizasiya — ACL (lab-da etdik)

- `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
- `allow.everyone.if.no.acl.found=false` — **deny by default**: ACL yoxdursa, heç nə olmaz.
- `super.users=User:broker;User:admin` — bunlara ACL lazım deyil.
- **ACL** = *principal* (`User:cbar-producer`) + *resource* (topic / group / cluster /
  transactional-id) + *operation* (Read / Write / Describe / Create / ...) + *permission* (Allow/Deny).

Lab-dakı ACL-lər (`infra/kafka-vm/node/kafka-security.sh`):

| Principal | Resurs | Əməliyyatlar |
|---|---|---|
| `cbar-producer` | topic `*` | Write, Describe, Create, DescribeConfigs |
| `cbar-integration-hub` | topic `*` | Read, Describe |
| `cbar-integration-hub` | group `*` | Read |
| `kafka-ui` | cluster, topic `*`, group `*`, transactional-id `*` | All |

**Real mühitdə fərq — least privilege:** lab-da hesablar bütün topic-lərə açıqdır (sadəlik üçün).
Real mühitdə **hər tətbiqə ayrıca hesab** və yalnız öz topic-ləri / group-u:

```bash
# producer: yalnız öz topic-inə yazır
kafka-acls.sh ... --add --allow-principal User:payments-producer \
  --topic payments.v1 --operation Write --operation Describe
# consumer: yalnız öz topic-i və öz group-u
kafka-acls.sh ... --add --allow-principal User:payments-consumer \
  --topic payments.v1 --operation Read --operation Describe
kafka-acls.sh ... --add --allow-principal User:payments-consumer \
  --group payments-consumer --operation Read
# bir neçə topic üçün prefix: --resource-pattern-type prefixed --topic payments.
```

Lab-da yoxladıq: hesabsız client heç nə ala bilmir; səhv parol → `Authentication failed`;
producer hesabı oxuya bilmir, consumer hesabı yaza və topic yarada bilmir → `AuthorizationException`.

**Topic yaratmaq kimin işidir?** Lab-da producer tətbiqi topic-i kodla yaradır (`Create` icazəsi var).
Real mühitdə adətən topic-ləri **platform komandası** yaradır (tiket + IaC), tətbiqlərdə `Create`
icazəsi olmur.

### 7.5 Parolların / secret-lərin idarəsi

| Lab-da | Real mühitdə |
|---|---|
| `infra/kafka-vm/secrets.local`, `infra/ldap/secrets.local` (git-ignored, təsadüfi generasiya) | **HashiCorp Vault / CyberArk** |
| Tətbiq parolları `application.yml`-da açıq (lab qərarı, repo public-dir) | **Heç vaxt** git-də deyil: env var, Vault agent, K8s Secret |
| Parol rotation yoxdur | Rotation prosesi: yeni parol → tətbiqə ötür → köhnəni sil |

### 7.6 Kafka UI-a giriş — LDAP + RBAC (lab-da etdik)

- **LDAP** — istifadəçi kataloqu ilə danışmaq protokolu. Bankda bu **Active Directory**-dir.
- Login: Kafka UI istifadəçinin adı və parolu ilə LDAP-a **bind** edir (parolu özü saxlamır).
- Qruplar: ayrıca **read-only servis hesabı** (lab-da `kafka-ui-bind`) ilə LDAP-dan oxunur.
- **RBAC**: AD qrupu → Kafka UI rolu.

| AD qrupu (lab-da) | Rol | İcazə |
|---|---|---|
| `kafka-admins` | admins | Hər şey |
| `kafka-viewers` | viewers | Broker/topic/partition/lag-a baxmaq; **mesaj məzmununu oxumaq yox** |

Mesajlarda şəxsi və maliyyə məlumatı ola bilər — `MESSAGES_READ` yalnız lazım olanlara verilir.

DevOps / AD komandasından istəniləcək:
- AD qrupları: `kafka-admins`, `kafka-viewers` (adlandırma bankın standartına görə).
- Kafka UI üçün **servis hesabı** (bind user), yalnız oxuma hüququ ilə, parolu Vault-da.
- **LDAPS** (636) ünvanı və AD-nin CA sertifikatı.
- User search base / group search base (məs. `OU=Users,DC=cbar,DC=az`).

### 7.7 Audit
- Kim nə vaxt hansı ACL-i dəyişdi, kim topic sildi — Kafka-nın authorizer log-u
  (`kafka-authorizer.log`) mərkəzi log sisteminə (ELK/Splunk/SIEM) göndərilməlidir.
- Kafka UI-ın audit funksiyası da aktivləşdirilə bilər.

---

## 8. Klasterin ilk dəfə işə salınması (bootstrap)

Lab-da `scripts/05-bootstrap-cluster.ps1` + `node/format-storage.sh`.

1. **Cluster ID** bir dəfə yaradılır: `kafka-storage.sh random-uuid`.
2. Hər serverdə storage **eyni cluster ID ilə** format olunur, super user-lərin SCRAM məlumatı ilə:
   ```bash
   kafka-storage.sh format --config server.properties --cluster-id <ID> \
     --add-scram 'SCRAM-SHA-512=[name=broker,password=...]' \
     --add-scram 'SCRAM-SHA-512=[name=admin,password=...]'
   ```
   Format **bir dəfəlik** əməliyyatdır; işləyən klasterdə təkrarlanmır (`--ignore-formatted`).
3. Hər 3 server **eyni vaxtda** işə salınır (quorum leader seçə bilsin).
4. Tətbiq hesabları və ACL-lər yaradılır (`06-security.ps1`).
5. Yoxlama (`07-verify.ps1`): servis vəziyyəti, quorum (leader + 3 voter, lag 0), test topic RF=3,
   under-replicated partition = 0.

**Mövcud klasteri PLAINTEXT-dən SASL-a keçirmək** (lab-da `migrate-to-sasl.ps1`) — lab-da qısa
dayanma ilə etdik. Real mühitdə **dayanmasız (zero downtime)** yol, hər mərhələ ayrıca rolling restart:
1. SASL listener-i PLAINTEXT-in yanına əlavə et.
2. `inter.broker.listener.name`-i SASL-a keçir.
3. Tətbiqləri keçir, authorizer-i aktivləşdir, PLAINTEXT-i sil.

---

## 9. Tətbiqlərin qoşulması (producer / consumer)

Lab-da: `kafka-producer` (REST → Kafka) və `cbar-integration-hub` (`@KafkaListener`), Spring Boot 4.1.

### 9.1 Bağlantı parametrləri (real mühit)

```yaml
spring:
  kafka:
    bootstrap-servers: kafka-1.<domain>:9092,kafka-2.<domain>:9092,kafka-3.<domain>:9092
    security:
      protocol: SASL_SSL                     # lab-da SASL_PLAINTEXT
    ssl:
      trust-store-location: file:/etc/app/truststore.p12
      trust-store-password: ${TRUSTSTORE_PASSWORD}
      trust-store-type: PKCS12
    properties:
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
```

### 9.2 Producer — mesaj itməsin deyə
| Parametr | Dəyər | Niyə |
|---|---|---|
| `acks` | all | Bütün in-sync replikalar yazsın |
| `enable.idempotence` | true | Retry zamanı dublikat olmasın |
| `delivery.timeout.ms` | 30000 | Bu müddətdə alınmasa → xəta |
| `max.block.ms` | 10000 | Metadata gözləmə limiti |
| `send()` nəticəsi | **mütləq yoxlanılır** | Lab-da console producer xətanı udub keçdi və 1 mesaj itdi |

### 9.3 Consumer — mesaj emal olunmadan "oxundu" sayılmasın
| Parametr | Dəyər | Niyə |
|---|---|---|
| `enable-auto-commit` | false | Offset taymerlə yox, emaldan sonra commit olunur |
| `ack-mode` | record | Hər mesajdan sonra commit (at-least-once) |
| `concurrency` | = partition sayı | Hər partition-a bir thread |
| Error handler | retry + DLT | Bir pis mesaj partition-u bloklamasın |

**At-least-once** = mesaj itmir, amma nadir hallarda **iki dəfə** gələ bilər → consumer
**idempotent** olmalıdır (məs. ödəniş ID-si ilə dublikatı yoxlamaq).

### 9.4 Key və partition
- Eyni key → eyni partition → həmin key üçün **sıra qorunur** (məs. hesab nömrəsi key olsa,
  bir hesabın əməliyyatları ardıcıl emal olunur).
- Partition sayı yalnız **artırıla** bilər, azaldıla bilməz; artırılanda key → partition xəritəsi
  dəyişir. Ona görə əvvəlcədən düzgün planlaşdırılmalıdır.

---

## 10. Kafka UI

Lab-da: Kafbat Kafka UI 1.5.0, Windows-da jar, `127.0.0.1:8090`.

| | Lab-da | Real mühitdə |
|---|---|---|
| Harada | Developer laptopu | Ayrıca VM və ya Kubernetes |
| Giriş | `127.0.0.1` | HTTPS reverse proxy (nginx), daxili şəbəkə |
| Login | lldap (LDAP) | AD (LDAPS) və ya SSO (OAuth2) |
| Rollar | admins, viewers | + bəlkə `operators` (offset reset, amma topic silmə yox) |
| Kafka-ya qoşulma | `kafka-ui` SCRAM hesabı | Eyni, SASL_SSL ilə |
| `readOnly` | false (icazələri RBAC verir) | Production klasterində çox vaxt `true` |

---

## 11. Monitorinq və alerting

Lab-da yoxdur (növbəti addım). Real mühitdə **istismara verilməzdən əvvəl** hazır olmalıdır.

**Stack:** JMX exporter (hər brokerdə Java agent) → **Prometheus** → **Grafana**; alertlər
Alertmanager → e-poçt / Teams / PagerDuty. Log-lar → ELK / Splunk.

| Metrik | Normal | Alert |
|---|---|---|
| `UnderReplicatedPartitions` | 0 | > 0, 5 dəqiqədən çox |
| `OfflinePartitionsCount` | 0 | > 0 — **kritik** |
| `UnderMinIsrPartitionCount` | 0 | > 0 — yazmalar dayanıb |
| `ActiveControllerCount` (cəmi) | 1 | ≠ 1 |
| ISR shrink / expand sürəti | ~0 | Tez-tez dəyişirsə — şəbəkə/disk problemi |
| Consumer lag | Sabit / az | Davamlı artırsa |
| Request latency (Produce p99) | ms | Kəskin artım |
| Disk doluluğu | < 70% | > 75% |
| JVM GC pause, heap | — | Uzun pauzalar |
| Broker prosesi / `NRestarts` | — | Restart oldu (OOM?) |
| TLS sertifikatının müddəti | — | < 30 gün |

---

## 12. Gündəlik əməliyyatlar (Day-2 operations)

### 12.1 Rolling restart (konfiqurasiya dəyişikliyi, patch, sertifikat yeniləmə)
Bir anda **bir server**:
1. `systemctl restart kafka` (controlled shutdown leadership-i başqasına ötürür).
2. Gözlə: `UnderReplicatedPartitions = 0`, ISR tam.
3. Sonra növbəti server.

Lab-da VM-ləri 1 GB-dan 1.5 GB-a məhz belə böyütdük — data itmədi, ISR hər dəfə 40–63 saniyəyə
bərpa olundu.

### 12.2 Versiya yeniləmə (upgrade)
Yeni versiyanı ayrıca qovluğa aç → konfiqurasiyanı yoxla → symlink-i dəyiş → rolling restart →
hamısı yeni versiyada stabil olandan sonra `metadata.version`-u yüksəlt. Release notes mütləq oxunur.

### 12.3 Bütün klasteri dayandırmaq (planlı)
Brokerləri **paralel** dayandır. Ardıcıl dayandıranda sonuncular quorum-u gözləyir və
`TimeoutStopSec`-ə (120 san) qədər asılı qalır, sonra `SIGKILL` alır — lab-da baş verdi.

### 12.4 Preferred leader election
Server qayıdandan sonra leader-lər qeyri-bərabər qalır. `auto.leader.rebalance.enable=true`
≤5 dəqiqəyə düzəldir, və ya dərhal:
`kafka-leader-election.sh --election-type PREFERRED --all-topic-partitions`.

### 12.5 Yeni tətbiq / topic sorğusu (proses)
Tətbiq komandası tiketlə istəyir: topic adı, partition sayı, retention, gözlənilən həcm,
producer/consumer hesabları → platform komandası yaradır (IaC, review) → ACL → parollar Vault-a →
tətbiq komandasına bildiriş.

---

## 13. Qəbul testləri (acceptance / failure drills)

Klaster istismara verilməzdən əvvəl bu testlər real mühitdə də aparılmalıdır
(lab-da `scripts/08-drills.ps1`):

| # | Test | Gözlənilən | Lab nəticəsi |
|---|---|---|---|
| 1 | 1 server dayandırılır | ISR 3→2, yazma davam edir | 500/500 ✅ |
| 2 | 2 server dayandırılır | Yazma dayanır (`NOT_ENOUGH_REPLICAS`), data itmir | 0/300 ✅ |
| 3 | Serverlər qayıdır | ISR 3, təsdiqlənmiş heç bir mesaj itməyib | 500/500 ✅ |
| 4 | Broker prosesi `kill -9` | systemd yenidən başladır, yazma davam edir | 300/300 ✅ |
| 5 | Hesabsız / səhv parol | Qoşula bilmir | ✅ |
| 6 | Tətbiq icazəsi olmayan əməliyyat | `AuthorizationException` | ✅ |
| 7 | Rolling restart | Data itkisi yoxdur | ✅ |
| 8 | Disk dolması, şəbəkə bölünməsi | Alert gəlir | Real mühitdə |
| 9 | DR failover | RPO/RTO daxilində | Real mühitdə |

---

## 14. DR (Disaster Recovery)

- Bir klaster **bir DC daxilində** server itkisinə dözür, amma **bütün DC** gedəndə yox.
- **İkinci DC-də** ayrıca klaster + **MirrorMaker 2** (MM2) ilə replikasiya:
  topic-lər, consumer offset-ləri (translation) və ACL-lər.
- Müzakirə olunmalı rəqəmlər (tənzimləyici tələb ola bilər):
  - **RPO** (Recovery Point Objective) — nə qədər data itkisi qəbul olunur (MM2 asinxrondur → saniyələr).
  - **RTO** (Recovery Time Objective) — nə qədər müddətə bərpa olunmalıdır.
- Failover və failback prosesi yazılmalı və **sınanmalıdır**.
- Kafka "backup" etmək əvəzinə adətən **replikasiya + DR klaster** istifadə olunur.

---

## 15. Lab-da öyrəndiyimiz dərslər

| Nə oldu | Nəticə | Real mühit üçün dərs |
|---|---|---|
| Broker VM-i restart oldu, DHCP yeni IP verdi | Broker klasterə qayıda bilmədi | Statik IP + DNS |
| 1 GB-lıq VM-də broker + CLI JVM | OOM killer brokeri öldürdü | RAM-ı düzgün hesabla; serverdə əlavə JVM-lərə diqqət |
| Brokerlər ardıcıl dayandırıldı | Hər biri 120 san asılı qaldı, `SIGKILL` | Tam dayandırma paralel; rolling restart bir-bir |
| Producer-in ilk sorğusu ölü brokerə getdi | 1 mesaj itdi, console producer xətanı uddu | `send()` callback-i mütləq yoxla; alert / retry / DLT |
| `acks=all` timeout aldı, amma mesaj sonradan göründü | Retry → dublikat riski | Idempotent producer + idempotent consumer |
| Quorum itdi | Producer xəta vermədi, asılı qaldı | Tətbiqdə timeout / circuit breaker; monitorinq |
| `netplan apply` | SSH sessiyası qırıldı | Şəbəkə dəyişikliyi yalnız lazım olan interfeysə |
| Konfiqurasiya faylında parol | `0640 root:kafka` | Secret-li fayllar ən az icazə ilə |

---

## 16. DevOps-a göndəriləcək sorğular (checklist)

### Infrastruktur / compute
- [ ] 3 broker server (və ya 3 broker + 3 controller): vCPU, RAM, disk — bölmə 3.1
- [ ] Serverlər **fərqli fiziki host / rack / AZ**-da (anti-affinity) — rack adları
- [ ] Ayrıca data disk(lər), SSD, XFS, `noatime`; tutum hesabı əlavə edilib
- [ ] OS: \<bankın standartı\>, Java 21, NTP/chrony
- [ ] Kernel parametrləri və `nofile` limiti — bölmə 3.3
- [ ] `kafka` servis istifadəçisi; admin-lər üçün `sudo` qaydaları

### Şəbəkə
- [ ] Statik IP-lər; DNS A + PTR qeydləri (FQDN)
- [ ] Firewall qaydaları — bölmə 4.2 (port matrisi)
- [ ] Brokerlər arası latency, idle timeout yoxlaması

### Artifact-lar
- [ ] Kafka tarball (versiya) + SHA512 daxili repoda (Nexus/Artifactory)
- [ ] JMX exporter jar daxili repoda

### Təhlükəsizlik
- [ ] Hər broker üçün TLS sertifikatı (CN/SAN = FQDN), CA zənciri, PKCS12
- [ ] Sertifikat yeniləmə prosesi və müddət alerti
- [ ] AuthN mexanizmi qərarı: SCRAM / Kerberos / OAuth
- [ ] Super user hesabları (`broker`, operator admin) — parollar Vault-da
- [ ] Hər tətbiq üçün ayrıca hesab + ACL (least privilege); parollar Vault-da
- [ ] AD qrupları (`kafka-admins`, `kafka-viewers`), Kafka UI üçün LDAP bind servis hesabı, LDAPS
- [ ] Authorizer / audit log-larının SIEM-ə göndərilməsi

### Monitorinq və log
- [ ] Prometheus scrape (JMX exporter portu), Grafana dashboard, alert qaydaları — bölmə 11
- [ ] Log-ların mərkəzi sistemə göndərilməsi (`/var/log/kafka`)

### Proses və sənədləşmə
- [ ] Topic / ACL sorğusu üçün tiket şablonu və təsdiq axını
- [ ] Rolling restart, upgrade, sertifikat yeniləmə runbook-ları
- [ ] Qəbul testləri (bölmə 13) — kim, nə vaxt aparır
- [ ] DR: ikinci DC, MM2, RPO/RTO, failover testi
- [ ] On-call / eskalasiya: kim alert alır

---

## 17. Terminlər lüğəti

| Termin | İzah |
|---|---|
| **Broker** | Mesajları saxlayan və verən Kafka serveri |
| **Controller** | Klasterin metadata-sını idarə edən rol (leader seçimi, topic-lər, ACL-lər) |
| **KRaft** | Kafka-nın ZooKeeper-siz metadata rejimi (Raft protokolu) |
| **Quorum** | Qərar üçün lazım olan çoxluq; 3 controller-dən 2-si |
| **Combined mode** | Bir serverdə həm broker, həm controller |
| **Topic** | Mesajların məntiqi kanalı (məs. `cbar.messages`) |
| **Partition** | Topic-in hissəsi; paralellik və sıra vahidi |
| **Offset** | Mesajın partition daxilindəki sıra nömrəsi |
| **Replica / RF** | Partition-un surəti / surət sayı (Replication Factor) |
| **Leader / Follower** | Partition-a yazılan əsas replika / onu kopyalayanlar |
| **ISR** | In-Sync Replicas — leader-ə çatmış replikalar |
| **URP** | Under-Replicated Partitions — ISR-i tam olmayan partition-lar |
| **min.insync.replicas** | `acks=all` yazış üçün minimum ISR sayı |
| **acks** | Producer təsdiqi: 0 / 1 / all |
| **Idempotent producer** | Retry-də dublikat yaratmayan producer |
| **Consumer group** | Partition-ları öz aralarında bölüşən consumer-lər |
| **Consumer lag** | Consumer-in son mesajdan nə qədər geridə qaldığı |
| **Rebalance** | Consumer group-da partition-ların yenidən bölüşdürülməsi |
| **At-least-once** | Mesaj itmir, amma təkrar gələ bilər |
| **DLT / DLQ** | Dead Letter Topic/Queue — emal oluna bilməyən mesajlar üçün |
| **Retention** | Mesajların saxlanma müddəti |
| **broker.rack / rack awareness** | Replikaları fərqli rack-lara paylamaq |
| **Unclean leader election** | Geridə qalmış replikanın leader olması (data itkisi riski) |
| **Controlled shutdown** | Broker dayanmazdan əvvəl leadership-i ötürür |
| **Rolling restart** | Serverləri bir-bir restart etmək |
| **Preferred leader** | Partition-un ilk replikası; balans üçün leader olmalıdır |
| **Listener** | Brokerin dinlədiyi ünvan + protokol (`SASL_SSL://:9092`) |
| **advertised.listeners** | Brokerin client-lərə "mənə bu ünvanla qoşul" dediyi ünvan |
| **SASL** | Autentifikasiya çərçivəsi (SCRAM, GSSAPI, PLAIN, OAUTHBEARER) |
| **SCRAM-SHA-512** | Parolu göndərmədən, challenge-response ilə autentifikasiya |
| **SASL_PLAINTEXT / SASL_SSL** | SASL şifrələməsiz / SASL + TLS |
| **TLS / mTLS** | Şifrələmə / hər iki tərəfin sertifikatla autentifikasiyası |
| **Keystore / Truststore** | Öz sertifikatı + açarı / etibar edilən CA-lar |
| **CN / SAN** | Sertifikatdakı ad / alternativ adlar (FQDN-lər) |
| **Principal** | Kafka-da autentifikasiya olunmuş kimlik (`User:cbar-producer`) |
| **ACL** | Principal + resurs + əməliyyat + icazə qaydası |
| **Authorizer** | ACL-ləri yoxlayan komponent (`StandardAuthorizer`) |
| **Super user** | ACL yoxlamasından keçməyən hesab |
| **Deny by default** | ACL yoxdursa, icazə də yoxdur |
| **Least privilege** | Hər hesaba yalnız lazım olan minimum icazə |
| **LDAP / AD** | İstifadəçi kataloqu protokolu / Microsoft-un kataloq serveri |
| **Bind** | LDAP-a istifadəçi adı + parolla autentifikasiya |
| **RBAC** | Rol əsaslı icazə (qrup → rol → icazələr) |
| **Service account** | İnsan yox, tətbiq üçün hesab |
| **Vault** | Secret-lərin (parol, açar) mərkəzi saxlanma sistemi |
| **JMX exporter** | Kafka metriklərini Prometheus formatında verən agent |
| **MirrorMaker 2** | Klasterlər arası replikasiya aləti (DR üçün) |
| **RPO / RTO** | Qəbul olunan data itkisi / bərpa müddəti |
| **IaC** | Infrastructure as Code — infrastruktur kodla və review ilə |
| **Runbook** | Əməliyyatın addım-addım təlimatı |
