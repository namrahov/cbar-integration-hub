#!/usr/bin/env bash
# Installs lldap as a systemd service. Idempotent.
# Usage: install-lldap.sh <version> <sha256> <bind_ip> <lldap.env file>
set -euo pipefail

VERSION="$1"
SHA256="$2"
BIND_IP="$3"
ENV_SRC="$4"

HERE="$(cd "$(dirname "$0")" && pwd)"
DIR="/opt/lldap-$VERSION"
URL="https://github.com/lldap/lldap/releases/download/v$VERSION/amd64-lldap.tar.gz"

sed -i 's/\r$//' "$HERE"/*.sh "$HERE"/*.service "$ENV_SRC"

echo "== packages"
export DEBIAN_FRONTEND=noninteractive
apt-get install -y -qq curl jq >/dev/null

echo "== service user and directories"
id lldap &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin lldap
install -d -o lldap -g lldap -m 0750 /var/lib/lldap
install -d -o root -g lldap -m 0750 /etc/lldap

echo "== lldap $VERSION"
if [ ! -d "$DIR" ]; then
  tmp="$(mktemp -d)"
  curl -fsSL -o "$tmp/lldap.tgz" "$URL"
  echo "$SHA256  $tmp/lldap.tgz" | sha256sum -c --quiet
  mkdir -p "$DIR"
  tar -xzf "$tmp/lldap.tgz" -C "$DIR" --strip-components=1
  rm -rf "$tmp"
fi
chown -R root:root "$DIR"
ln -sfn "$DIR" /opt/lldap

echo "== configuration (listening on $BIND_IP only)"
{
  tr -d '\r' < "$ENV_SRC"
  echo "LLDAP_LDAP_HOST=$BIND_IP"
  echo "LLDAP_HTTP_HOST=$BIND_IP"
  echo "LLDAP_HTTP_URL=http://$BIND_IP:17170"
} > /etc/lldap/lldap.env
chown root:lldap /etc/lldap/lldap.env
chmod 0640 /etc/lldap/lldap.env
rm -f "$ENV_SRC"

install -m 0644 "$HERE/lldap.service" /etc/systemd/system/lldap.service
systemctl daemon-reload
systemctl enable --now lldap
systemctl restart lldap

for _ in $(seq 1 30); do
  curl -sf "http://$BIND_IP:17170/health" >/dev/null && { echo "== lldap is up"; exit 0; }
  sleep 1
done
journalctl -u lldap --no-pager -n 30
exit 1
