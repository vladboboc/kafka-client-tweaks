#!/bin/bash
# Topic provisioning + feature check. Runs once inside a cp-kafka container after all brokers are healthy.
#
# auto.create.topics.enable is off on the brokers so that a typo in a topic name fails loudly instead
# of silently creating a 1-partition topic with default settings. The demos can also (re)create their
# own topics through AdminClient; this script makes sure the baseline set exists and is visible in the
# UI before you run anything.
set -euo pipefail

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"

create() {
  local topic="$1" partitions="$2"; shift 2
  local extra=()
  for cfg in "$@"; do extra+=(--config "$cfg"); done
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    "${extra[@]}" >/dev/null
  echo "  ok: $topic ($partitions partitions, RF=3${*:+, $*})"
}

echo "waiting for all three brokers to register..."
for _ in $(seq 1 30); do
  count=$(kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -c 'id: ' || true)
  [ "${count:-0}" -ge 3 ] && break
  sleep 2
done

echo "creating topics..."
#      name                   partitions  topic-level overrides
create tweaks.baseline        3
create tweaks.batching        3
create tweaks.durability      3   min.insync.replicas=2
create tweaks.durability-isr1 3   min.insync.replicas=1
create tweaks.partitioning    6
create tweaks.latency         3
create tweaks.txn-in          3
create tweaks.txn-out         3
create tweaks.fetch           3
create tweaks.offsets         3
create tweaks.rebalance       6
create tweaks.parallel        6
create tweaks.queue           3
create tweaks.resilience      3
create tweaks.avro            3

echo
echo "checking share groups (Queues for Kafka, KIP-932)..."
# Kafka 4.2+ formats new clusters with share.version=1 (production ready). If the cluster metadata
# was formatted at a lower level the finalized version is 0 and KafkaShareConsumer would fail with
# UNSUPPORTED_VERSION, so upgrade the feature explicitly.
# (pure bash: the cp-kafka image is a slim UBI build without awk)
finalized=""
while IFS= read -r line; do
  if [[ "$line" == Feature:*share.version* && "$line" =~ FinalizedVersionLevel:\ *([0-9]+) ]]; then
    finalized="${BASH_REMATCH[1]}"
  fi
done < <(kafka-features --bootstrap-server "$BOOTSTRAP" describe 2>/dev/null)
echo "  share.version finalized level: ${finalized:-unknown}"
if [ "${finalized:-0}" -lt 1 ]; then
  kafka-features --bootstrap-server "$BOOTSTRAP" upgrade --feature share.version=1
  echo "  share.version upgraded to 1"
fi

echo
echo "creating a KIP-714 client metrics subscription (chapter 12)..."
# With a subscription in place every producer/consumer that has enable.metrics.push=true (the default) gets a
# client instance id from the broker and pushes the listed metrics every 10 s. Collecting them centrally needs a
# broker-side metrics reporter plugin; without one this only demonstrates the mechanism.
kafka-client-metrics --bootstrap-server "$BOOTSTRAP" --alter --name tweaks-all-clients \
  --metrics org.apache.kafka.producer.,org.apache.kafka.consumer. --interval 10000 >/dev/null \
  && echo "  ok: subscription tweaks-all-clients (all clients, producer + consumer metrics, every 10 s)"

echo
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
echo "topic provisioning complete"
