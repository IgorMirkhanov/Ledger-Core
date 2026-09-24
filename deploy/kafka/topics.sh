#!/usr/bin/env bash
# Creates Kafka topics explicitly (production runs with KAFKA_AUTO_CREATE_TOPICS=false).
# Usage: BOOTSTRAP=kafka-0.kafka:9092 ./deploy/kafka/topics.sh   (requires kafka-topics.sh or rpk)
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-localhost:19092}"
RF="${REPLICATION_FACTOR:-3}"

# topic:partitions:retention.ms
TOPICS=(
  "ledger.accounts.v1:12:604800000"        # 7 days; key = account/hold id keeps per-aggregate order
  "ledger.transfers.v1:12:604800000"
  "ledger.notifications.dlq:3:2592000000"  # 30 days for manual replay
)

for spec in "${TOPICS[@]}"; do
  IFS=: read -r name parts retention <<<"$spec"
  if command -v rpk >/dev/null; then
    rpk topic create "$name" -X brokers="$BOOTSTRAP" -p "$parts" -r "$RF" \
      -c retention.ms="$retention" -c min.insync.replicas=2 || true
  else
    kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$name" \
      --partitions "$parts" --replication-factor "$RF" \
      --config retention.ms="$retention" --config min.insync.replicas=2
  fi
done
echo "topics ready"
