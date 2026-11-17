"""Kafka consumer: validates events against the contract, updates the streaming metrics in MySQL.

Delivery semantics (documented in the README, section 12):
  * Kafka -> consumer is AT-LEAST-ONCE: offsets are committed manually, only AFTER the MySQL
    transaction has committed.
  * Effect on the metrics is EXACTLY-ONCE: every valid event_id is first written to
    ``stream_eventos_procesados`` inside the same transaction that updates the accumulators, so a
    replayed message (producer retry, consumer crash between commit and offset commit) is detected
    and counted as a duplicate instead of being added twice.
  * An invalid event never stops the stream: it is stored in ``stream_eventos_invalidos`` and
    forwarded to the DLQ topic.
  * If MySQL fails the consumer exits (non-zero) WITHOUT committing offsets; docker restarts it and
    it resumes from the last committed offset.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.load import db as dw_db
from src.streaming import contract, metrics, settings, store

log = logging.getLogger("stream.consumer")


@dataclass
class BatchResult:
    n_messages: int = 0
    n_valid: int = 0
    n_duplicate: int = 0
    n_invalid: int = 0
    n_suspicious: int = 0
    n_sin_referencia: int = 0
    dlq_records: list[dict] = field(default_factory=list)


def _decode(raw: bytes | None, limit: int | None = None) -> str:
    text = (raw or b"").decode("utf-8", errors="replace")
    return text[:limit] if limit else text


def classify(messages: list) -> tuple[list[contract.Saber11ResultEvent], list[dict]]:
    """Splits raw Kafka messages into contract-valid events and rejected records (with the reason)."""
    valid, invalid = [], []
    for m in messages:
        key = _decode(m.key())
        payload = _decode(m.value(), 4000)
        try:
            if m.value() is None:
                raise ValueError("empty payload (tombstone)")
            ev = contract.parse_event(m.value())
            if key != ev.cod_dane_depto:  # the Kafka key IS the department: ordering depends on it
                raise ValueError(f"message key '{key}' differs from cod_dane_depto '{ev.cod_dane_depto}'")
            valid.append(ev)
        except Exception as exc:  # noqa: BLE001 - any failure to parse/validate is a contract violation
            invalid.append({"partition": m.partition(), "offset": m.offset(), "key": key,
                            "motivo": contract.describe_error(exc), "payload": payload})
    return valid, invalid


def process_batch(conn, messages: list, reference_keys: set[tuple[str, int]],
                  now: datetime | None = None) -> BatchResult:
    """Applies one micro-batch in ONE transaction. The caller commits the Kafka offsets afterwards.

    ``now`` is a naive UTC datetime (injectable for tests).
    """
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    result = BatchResult(n_messages=len(messages))
    valid, invalid = classify(messages)

    try:
        with conn.cursor() as cur:
            # --- idempotency: drop what was already applied (earlier runs) and repeats inside the batch
            already = store.existing_event_ids(cur, [e.event_id for e in valid])
            seen: set[str] = set()
            fresh: list[contract.Saber11ResultEvent] = []
            for ev in valid:
                if ev.event_id in already or ev.event_id in seen:
                    result.n_duplicate += 1
                else:
                    seen.add(ev.event_id)
                    fresh.append(ev)

            # --- invalid events: only the ones not logged before count (re-read after a crash)
            new_invalid = store.log_invalid(cur, invalid, now)
            result.n_invalid = len(new_invalid)
            result.dlq_records = new_invalid

            # --- metrics
            agg = metrics.aggregate(fresh, reference_keys, now)
            result.n_valid = agg.n_events
            result.n_suspicious = agg.n_suspicious
            result.n_sin_referencia = agg.n_sin_referencia
            if fresh:
                store.apply_aggregate(cur, agg, [e.event_id for e in fresh], now)
            store.update_state(cur, n_valid=result.n_valid, n_invalid=result.n_invalid, n_dup=result.n_duplicate,
                               n_sin_ref=result.n_sin_referencia, last_event_ts=agg.last_event_ts, now=now)
            snapshot = store.national_snapshot(cur)
            store.upsert_time_bucket(
                cur, metrics.bucket_of(now), n_valid=result.n_valid, n_invalid=result.n_invalid,
                n_dup=result.n_duplicate, n_susp=result.n_suspicious,
                sum_latency_ms=agg.sum_latency_ms, n_latency=agg.n_latency, snapshot=snapshot)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return result


class StreamConsumer:
    def __init__(self, args: argparse.Namespace) -> None:
        from confluent_kafka import Consumer, Producer  # imported here: tests do not need librdkafka

        self.args = args
        self.running = True
        self.consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.CONSUMER_GROUP,
            "enable.auto.commit": False,          # offsets are committed AFTER the MySQL commit
            "auto.offset.reset": "earliest",      # a new group reads the whole topic
            "enable.partition.eof": False,
            "session.timeout.ms": 15000,
        })
        self.dlq = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True})
        self.conn = dw_db.get_connection()
        self.reference_keys: set[tuple[str, int]] = set()
        self._next_reference_refresh = 0.0

    # ------------------------------------------------------------------ helpers
    def _refresh_reference(self, force: bool = False) -> None:
        if not force and time.monotonic() < self._next_reference_refresh:
            return
        store.refresh_reference(self.conn, datetime.now(timezone.utc).replace(tzinfo=None))
        self.reference_keys = store.load_reference_keys(self.conn)
        self._next_reference_refresh = time.monotonic() + settings.REFERENCE_REFRESH_SECONDS
        if not self.reference_keys:
            log.warning("[REFERENCE] no MEN indicators available: run the Airflow DAG (API branch) first. "
                        "Events will be counted as 'sin referencia' and M6/M7 stay empty.")

    def _send_to_dlq(self, records: list[dict]) -> None:
        for r in records:
            body = {"error": r["motivo"], "source_topic": settings.TOPIC_RESULTS, "partition": r["partition"],
                    "offset": r["offset"], "original_payload": r["payload"],
                    "rejected_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds")}
            self.dlq.produce(settings.TOPIC_DLQ, key=(r["key"] or "").encode(), value=json.dumps(body, ensure_ascii=False))
        if records:
            self.dlq.flush(10)

    def stop(self, *_: object) -> None:
        log.info("shutdown requested: finishing the current micro-batch")
        self.running = False

    # --------------------------------------------------------------------- run
    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        store.init_schema(self.conn)  # idempotent: the consumer can start before stream-init
        self._refresh_reference(force=True)
        self.consumer.subscribe([settings.TOPIC_RESULTS])
        log.info("consuming '%s' as group '%s' (batch<=%d msgs or %.1fs, MEN reference: %d dept-year keys)",
                 settings.TOPIC_RESULTS, settings.CONSUMER_GROUP, self.args.max_batch, self.args.flush_seconds,
                 len(self.reference_keys))
        try:
            while self.running:
                msgs = self.consumer.consume(num_messages=self.args.max_batch, timeout=self.args.flush_seconds)
                self._refresh_reference()
                usable = []
                for m in msgs:
                    if m.error():
                        log.warning("kafka error: %s", m.error())
                    else:
                        usable.append(m)
                if not usable:
                    continue
                result = process_batch(self.conn, usable, self.reference_keys)
                self._send_to_dlq(result.dlq_records)
                self.consumer.commit(asynchronous=False)  # only after MySQL committed
                log.info("batch: %3d msgs -> %3d applied, %d duplicate, %d invalid%s",
                         result.n_messages, result.n_valid, result.n_duplicate, result.n_invalid,
                         f", {result.n_sin_referencia} without MEN reference" if result.n_sin_referencia else "")
        finally:
            self.consumer.close()
            self.conn.close()
            log.info("consumer closed")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Saber 11 streaming consumer (contract validation + live metrics)")
    ap.add_argument("--max-batch", type=int, default=settings.CONSUMER_MAX_BATCH)
    ap.add_argument("--flush-seconds", type=float, default=settings.CONSUMER_FLUSH_SECONDS)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    StreamConsumer(args).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
