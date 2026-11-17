#!/usr/bin/env bash
# The two experiments required by the assignment (section 2.6): invalid event and consumer interruption.
# Usage:  scripts/demo_experiments.sh invalid | interruption
set -euo pipefail
PERIOD="${PERIOD:-2024-2}"
GROUP="${KAFKA_CONSUMER_GROUP:-btb-stream-metrics}"
lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
    --describe --group "$GROUP" 2>/dev/null || true
}
sql() { docker compose exec -T mysql mysql -uroot -p"${MYSQL_ROOT_PASSWORD:-changeme}" icfes_dw -e "$1" 2>/dev/null; }

case "${1:-}" in
invalid)
  echo "== Experiment 1: invalid events =="
  echo "-- before:"; sql "SELECT n_validos, n_invalidos, n_duplicados FROM v_stream_estado"
  docker compose run --rm stream-producer --period "$PERIOD" --sample-pct 1 --limit 150 --rate 30 \
      --inject-invalid 8 --inject-duplicates 5
  sleep 3
  echo "-- after (8 invalid injected, 5 duplicates injected):"
  sql "SELECT n_validos, n_invalidos, n_duplicados FROM v_stream_estado"
  sql "SELECT kafka_partition, kafka_offset, LEFT(motivo, 90) AS motivo FROM stream_eventos_invalidos ORDER BY recibido_en DESC LIMIT 8"
  echo "-- DLQ topic (first messages):"
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
    --topic "${KAFKA_TOPIC_DLQ:-saber11.resultados.dlq.v1}" --from-beginning --max-messages 3 --timeout-ms 5000 || true
  ;;
interruption)
  echo "== Experiment 2: consumer temporarily stopped =="
  docker compose run -d --name btb_producer_demo stream-producer --period "$PERIOD" --sample-pct 1 --rate 20 >/dev/null
  sleep 15; echo "-- consumer running:"; sql "SELECT n_validos, segundos_sin_datos FROM v_stream_estado"
  echo ">> stopping the consumer for 30 s (the producer keeps publishing)"; docker compose stop stream-consumer
  sleep 30; echo "-- lag while stopped (see LAG column):"; lag
  sql "SELECT n_validos, segundos_sin_datos FROM v_stream_estado"
  echo ">> restarting the consumer"; docker compose start stream-consumer
  sleep 20; echo "-- lag after restart (should drop to ~0):"; lag
  docker wait btb_producer_demo >/dev/null; docker rm btb_producer_demo >/dev/null
  sleep 5
  echo "-- verification: applied events == distinct event_ids (no loss, no duplicates in the metrics)"
  sql "SELECT (SELECT n_validos FROM stream_estado WHERE id=1) AS n_validos, (SELECT COUNT(*) FROM stream_eventos_procesados) AS event_ids"
  ;;
*) echo "usage: $0 invalid | interruption"; exit 1;;
esac
