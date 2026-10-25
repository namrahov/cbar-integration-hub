#!/usr/bin/env bash
# Installs the Prometheus exporters on one node. Idempotent.
#   JMX exporter  : Java agent loaded by kafka.service (KAFKA_OPTS), serves :7071/metrics.
#                   Takes effect after the next broker restart.
#   node_exporter : own systemd service, serves :9100/metrics (CPU, memory, disk, network).
# Usage: install-exporters.sh <jmx version> <jmx sha256> <node_exporter version> <node_exporter sha256>
set -euo pipefail

JMX_VERSION="$1"
JMX_SHA256="$2"
NE_VERSION="$3"
NE_SHA256="$4"

HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== JMX exporter $JMX_VERSION"
JMX_DIR=/opt/jmx-exporter
JMX_JAR="$JMX_DIR/jmx_prometheus_javaagent-$JMX_VERSION.jar"
install -d -m 0755 "$JMX_DIR"
if [ ! -f "$JMX_JAR" ]; then
  curl -fsSL -o "$JMX_JAR.part" \
    "https://github.com/prometheus/jmx_exporter/releases/download/$JMX_VERSION/jmx_prometheus_javaagent-$JMX_VERSION.jar"
  echo "$JMX_SHA256  $JMX_JAR.part" | sha256sum -c --quiet
  mv "$JMX_JAR.part" "$JMX_JAR"
fi
ln -sfn "$JMX_JAR" "$JMX_DIR/jmx_prometheus_javaagent.jar"
install -m 0644 "$HERE/jmx-exporter.yml" /etc/kafka/jmx-exporter.yml

echo "== node_exporter $NE_VERSION"
NE_DIST="node_exporter-$NE_VERSION.linux-amd64"
if [ ! -d "/opt/$NE_DIST" ]; then
  tmp="$(mktemp -d)"
  curl -fsSL -o "$tmp/ne.tgz" \
    "https://github.com/prometheus/node_exporter/releases/download/v$NE_VERSION/$NE_DIST.tar.gz"
  echo "$NE_SHA256  $tmp/ne.tgz" | sha256sum -c --quiet
  tar -xzf "$tmp/ne.tgz" -C /opt
  rm -rf "$tmp"
fi
chown -R root:root "/opt/$NE_DIST"
ln -sfn "/opt/$NE_DIST" /opt/node_exporter
id node_exporter &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin node_exporter
install -m 0644 "$HERE/node_exporter.service" /etc/systemd/system/node_exporter.service
systemctl daemon-reload
systemctl enable --now node_exporter 2>/dev/null
systemctl restart node_exporter
