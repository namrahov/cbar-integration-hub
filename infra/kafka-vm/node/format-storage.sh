#!/usr/bin/env bash
# Formats this node's storage (one-time; formatted nodes are left alone) and seeds the
# SCRAM credentials of the super users, so brokers can authenticate to each other on first start.
# Usage: format-storage.sh <cluster id>
set -euo pipefail

CLUSTER_ID="$1"
# shellcheck disable=SC1091
source /etc/kafka/bootstrap-scram.env

# Run as the kafka user so the data dir stays owned by the service account
sudo -u kafka env LOG_DIR=/var/log/kafka /opt/kafka/bin/kafka-storage.sh format \
  --config /etc/kafka/server.properties --cluster-id "$CLUSTER_ID" --ignore-formatted \
  --add-scram "SCRAM-SHA-512=[name=broker,password=$BROKER_PASSWORD]" \
  --add-scram "SCRAM-SHA-512=[name=admin,password=$ADMIN_PASSWORD]" \
  | { grep -v BootstrapMetadata || true; }
