#!/usr/bin/env bash
# Creates/updates application SCRAM users and their ACLs. Idempotent. Runs as root (uses admin).
# Usage: kafka-security.sh <secrets file with PRODUCER_PASSWORD, CONSUMER_PASSWORD, KAFKA_UI_PASSWORD>
#
#   cbar-producer         all topics : Write, Describe, Create, DescribeConfigs
#                         (apps declare their topics in code, so the producer may create them)
#   cbar-integration-hub  all topics : Read, Describe
#                         all groups : Read
#   kafka-ui              everything; which human may do what is decided by Kafka UI's own RBAC
# Lab choice: shared producer/consumer accounts across topics. Production usually gives each
# application its own account limited to its own topics and group (least privilege).
set -euo pipefail

SECRETS="$1"
sed -i 's/\r$//' "$SECRETS"
# shellcheck disable=SC1090
source "$SECRETS"
rm -f "$SECRETS"

export LOG_DIR=/tmp/kafka-cli
export KAFKA_HEAP_OPTS="-Xmx128m"
B=/opt/kafka/bin
BS=kafka-1:9092,kafka-2:9092,kafka-3:9092
CFG=/etc/kafka/admin.properties
TOPIC=cbar.messages

scram_user() {  # name password
  $B/kafka-configs.sh --bootstrap-server $BS --command-config $CFG --alter \
    --entity-type users --entity-name "$1" \
    --add-config "SCRAM-SHA-512=[iterations=8192,password=$2]" >/dev/null
  echo "user $1: SCRAM-SHA-512 set"
}

acl() {  # kafka-acls arguments after --add
  $B/kafka-acls.sh --bootstrap-server $BS --command-config $CFG --add --force "$@" >/dev/null
}

scram_user cbar-producer "$PRODUCER_PASSWORD"
scram_user cbar-integration-hub "$CONSUMER_PASSWORD"
scram_user kafka-ui "$KAFKA_UI_PASSWORD"

# Drop the earlier per-topic grants; the wildcard ACLs below replace them
$B/kafka-acls.sh --bootstrap-server $BS --command-config $CFG --remove --force \
  --allow-principal User:cbar-producer --allow-principal User:cbar-integration-hub --topic $TOPIC >/dev/null 2>&1 || true
$B/kafka-acls.sh --bootstrap-server $BS --command-config $CFG --remove --force \
  --allow-principal User:cbar-integration-hub --group cbar-integration-hub >/dev/null 2>&1 || true

acl --allow-principal User:cbar-producer --topic '*' \
    --operation Write --operation Describe --operation Create --operation DescribeConfigs

acl --allow-principal User:cbar-integration-hub --topic '*' \
    --operation Read --operation Describe
acl --allow-principal User:cbar-integration-hub --group '*' --operation Read

acl --allow-principal User:kafka-ui --cluster --operation All
acl --allow-principal User:kafka-ui --topic '*' --operation All
acl --allow-principal User:kafka-ui --group '*' --operation All
acl --allow-principal User:kafka-ui --transactional-id '*' --operation All

echo "== ACLs"
$B/kafka-acls.sh --bootstrap-server $BS --command-config $CFG --list 2>/dev/null \
  | grep -E 'Current ACLs|principal=' | sed -E 's/\(principal=User:([^,]+), host=\*, operation=([A-Z_]+), permissionType=ALLOW\)/\1:\2/' \
  | tr -s ' \t' ' '
