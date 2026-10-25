#!/usr/bin/env bash
# Installs Kafka on one node. Idempotent: safe to re-run after config changes.
# Usage: install-kafka.sh <kafka_version> <scala_version> <node_id> <hostname> <rack>
set -euo pipefail

KAFKA_VERSION="$1"
SCALA_VERSION="$2"
NODE_ID="$3"
NODE_HOST="$4"
NODE_RACK="$5"

HERE="$(cd "$(dirname "$0")" && pwd)"
DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${DIST}.tgz"

# Guard against CRLF if files were edited on Windows
sed -i 's/\r$//' "$HERE"/*

echo "== [$NODE_HOST] packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq openjdk-21-jre-headless curl >/dev/null
java -version 2>&1 | head -1

echo "== [$NODE_HOST] OS tuning"
install -m 0644 "$HERE/99-kafka.conf" /etc/sysctl.d/99-kafka.conf
sysctl --quiet --system || echo "WARN: some sysctl settings were not applied" >&2
swapoff -a || true   # Kafka relies on page cache; swapping the JVM kills latency

echo "== [$NODE_HOST] service user and directories"
id kafka &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin kafka
install -d -o kafka -g kafka -m 0750 /var/lib/kafka/data /var/log/kafka
install -d -m 0755 /etc/kafka

echo "== [$NODE_HOST] Kafka $KAFKA_VERSION"
if [ ! -d "/opt/$DIST" ]; then
  tmp="$(mktemp -d)"
  curl -fsSL -o "$tmp/$DIST.tgz" "$URL"
  curl -fsSL -o "$tmp/$DIST.tgz.sha512" "$URL.sha512"
  # Apache publishes the hash as uppercase hex split over several lines
  expected="$(cut -d: -f2- "$tmp/$DIST.tgz.sha512" | tr -d ' \n\r' | tr 'A-F' 'a-f')"
  actual="$(sha512sum "$tmp/$DIST.tgz" | cut -d' ' -f1)"
  if [ "$expected" != "$actual" ]; then
    echo "SHA512 mismatch for $DIST.tgz" >&2
    exit 1
  fi
  tar -xzf "$tmp/$DIST.tgz" -C /opt
  rm -rf "$tmp"
fi
chown -R root:root "/opt/$DIST"
ln -sfn "/opt/$DIST" /opt/kafka   # upgrades = new dir + switch symlink

echo "== [$NODE_HOST] configuration (node.id=$NODE_ID, rack=$NODE_RACK)"
sed -e "s/__NODE_ID__/$NODE_ID/g" \
    -e "s/__NODE_HOST__/$NODE_HOST/g" \
    -e "s/__NODE_RACK__/$NODE_RACK/g" \
    "$HERE/server.properties.tmpl" > /etc/kafka/server.properties
chown root:kafka /etc/kafka/server.properties
chmod 0640 /etc/kafka/server.properties

install -m 0644 "$HERE/kafka.service" /etc/systemd/system/kafka.service
systemctl daemon-reload

install -m 0755 "$HERE/kafka-drill.sh" /usr/local/bin/kafka-drill

echo "== [$NODE_HOST] done"
