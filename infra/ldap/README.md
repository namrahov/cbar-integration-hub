# LDAP for Kafka UI (lldap)

[lldap](https://github.com/lldap/lldap) 0.6.3 runs as a systemd service on **kafka-1**, listening on
the lab IP only. Lab compromise: no RAM for a separate VM. In production this is Active Directory.

| Endpoint | Address                                   |
|----------|-------------------------------------------|
| Web UI   | http://10.10.10.11:17170                  |
| LDAP     | `ldap://10.10.10.11:3890`, base `dc=cbar,dc=lab` |

```powershell
.\infra\ldap\setup.ps1    # install/upgrade lldap, create groups and accounts (idempotent)
```

Secrets are generated once into `infra/ldap/secrets.local` (git-ignored):
`ADMIN_PASSWORD`, `BIND_PASSWORD`, `VIEWER_PASSWORD`, lldap JWT secret and key seed.

## Accounts and roles

| Account         | LDAP group              | Kafka UI role | Can do                                                    |
|-----------------|-------------------------|---------------|-----------------------------------------------------------|
| `admin`         | `kafka-admins`          | admins        | everything                                                |
| `viewer`        | `kafka-viewers`         | viewers       | see brokers, topics, partitions, configs, consumer lag    |
| `kafka-ui-bind` | `lldap_strict_readonly` | —             | service account Kafka UI uses to read group membership    |

Viewers **cannot read message contents** (`MESSAGES_READ` is not granted): payloads may contain
personal or financial data. They also cannot create/delete topics or produce messages (HTTP 403).

## Add a read-only user

1. Open http://10.10.10.11:17170 and log in as `admin` (`ADMIN_PASSWORD`).
2. **Create a user** → fill user ID (this is the login name), email, password.
3. **Groups** → `kafka-viewers` → add the user.
4. The user can log in to http://localhost:8090 right away. No Kafka UI restart needed.

To make someone an admin, add them to `kafka-admins` instead.
To let a viewer read message contents, add `MESSAGES_READ` to the viewers' topic actions in
`infra/kafka-ui/application.yml` (or create a separate role/group for it) and restart Kafka UI.
