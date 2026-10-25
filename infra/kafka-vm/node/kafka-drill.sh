#!/usr/bin/env bash
# Node-side helper for failure drills; installed as /usr/local/bin/kafka-drill.
# Usage: kafka-drill <create|desc|isr-full|produce <tag> <count>|count|quorum|mem>
# BS overrides the bootstrap list, e.g. BS=kafka-1:9092 kafka-drill count
set -uo pipefail

# The broker already uses most of a 1 GB VM: keep CLI JVMs small and their logs out of /opt
export LOG_DIR=/tmp/kafka-cli
export KAFKA_HEAP_OPTS="-Xmx128m"
B=/opt/kafka/bin
BS="${BS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
TOPIC=drill

case "${1:-}" in
  create)
    $B/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists --topic $TOPIC \
      --partitions 3 --replication-factor 3 --config min.insync.replicas=2 ;;
  desc)
    $B/kafka-topics.sh --bootstrap-server "$BS" --describe --topic $TOPIC 2>/dev/null \
      | tail -n +2 | awk '{print "partition="$4, "leader="$6, "replicas="$8, "isr="$10}' ;;
  isr-full)   # exit 0 when every partition has 3 in-sync replicas
    [ "$("$0" desc | grep -cE 'isr=[0-9],[0-9],[0-9]$')" -eq 3 ] ;;
  produce)    # produce <tag> <count>: acks=all + idempotence, hard-killed after 60s
    tag="$2"; cnt="$3"; start=$(date +%s)
    # SIGKILL: on SIGTERM the producer waits forever in close() for unacked records
    seq -f "$tag-%g" 1 "$cnt" | timeout -s KILL 60 $B/kafka-console-producer.sh \
      --bootstrap-server "$BS" --topic $TOPIC \
      --command-property acks=all --command-property enable.idempotence=true \
      --command-property delivery.timeout.ms=20000 --command-property request.timeout.ms=5000 \
      --command-property max.block.ms=10000 > /tmp/prod-$tag.log 2>&1
    rc=$?
    failed=$(grep -c 'Error when sending message' /tmp/prod-$tag.log)
    echo "producer exit=$rc took=$(( $(date +%s) - start ))s send-errors=$failed"
    grep -oE 'NOT_ENOUGH_REPLICAS[A-Z_]*|not present in metadata|TimeoutException' /tmp/prod-$tag.log \
      | sort | uniq -c | sed 's/^/  /' ;;
  count)
    $B/kafka-get-offsets.sh --bootstrap-server "$BS" --topic $TOPIC 2>/dev/null \
      | awk -F: '{s+=$3} END {print s+0}' ;;
  quorum)
    timeout -s KILL 30 $B/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status 2>&1 \
      | grep -E 'LeaderId|Exception' | cut -c1-120 ;;
  mem)
    free -m | grep Mem
    ps -eo rss,args --sort=-rss | grep '[j]ava' | cut -c1-80 ;;
  *)
    echo "usage: kafka-drill <create|desc|isr-full|produce <tag> <count>|count|quorum|mem>" >&2
    exit 2 ;;
esac
