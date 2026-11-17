#!/usr/bin/env bash
# Clean slate for a demo: empties the stream_* tables and rewinds the consumer group.
# The Data Warehouse is NOT touched. Run it with the stack up (docker compose up -d).
set -euo pipefail
GROUP="${KAFKA_CONSUMER_GROUP:-btb-stream-metrics}"
TOPIC="${KAFKA_TOPIC_RESULTS:-saber11.resultados.v1}"

echo "[1/4] stopping the consumer"
docker compose stop stream-consumer
echo "[2/4] emptying stream_* tables (Data Warehouse untouched)"
docker compose run --rm --entrypoint python stream-init -m src.streaming.init_db --reset
echo "[3/4] rewinding consumer group '$GROUP' to the beginning of '$TOPIC'"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-earliest --execute || true
echo "[4/4] starting the consumer"
docker compose up -d stream-consumer
echo "done. Messages already in the topic (retention 7 days) will be re-read and re-applied from zero."
