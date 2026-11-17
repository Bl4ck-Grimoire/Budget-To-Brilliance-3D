"""Kafka producer: REPLAYS historical records of ``fact_resultado_saber11`` as if they were arriving now.

This is a SIMULATION and it says so: the exam attempts already exist in the Data Warehouse, the
producer only paces their publication. Every event carries ``event_type = saber11.result.replayed``.

What is published
  * RECORDS (one exam attempt = one event), never repeated KPI values. The metrics are computed by the
    consumer, so a message always carries new information.
  * A deterministic sample of ONE period: ``MOD(CRC32(estu_consecutivo), 100) < sample_pct``. The same
    command always replays the same events in the same order (``RAND()`` would not be reproducible).

Experiment helpers (they publish EXTRA messages, they never alter the real ones)
  * ``--inject-invalid N``    N contract violations of several kinds -> consumer rejects them (DLQ)
  * ``--inject-duplicates N`` N exact copies of already published events -> consumer ignores them
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone

import pymysql

from src.load import db as dw_db
from src.streaming import contract, settings

log = logging.getLogger("stream.producer")

_SELECT_COLUMNS = """f.estu_consecutivo, p.periodo, d.cod_dane_depto,
       c.cole_naturaleza, c.cole_jornada, c.cole_area_ubicacion, g.estu_genero,
       f.punt_lectura_critica, f.punt_matematicas, f.punt_sociales_ciudadanas,
       f.punt_c_naturales, f.punt_ingles, f.punt_global, f.puntaje_sospechoso"""
_FROM_WHERE = """FROM fact_resultado_saber11 f
JOIN dim_periodo      p ON p.id_periodo      = f.id_periodo
JOIN dim_colegio      c ON c.id_colegio      = f.id_colegio
JOIN dim_departamento d ON d.id_departamento = c.id_departamento
JOIN dim_genero       g ON g.id_genero       = f.id_genero
WHERE p.periodo = %s AND MOD(CRC32(f.estu_consecutivo), 100) < %s"""
# Deterministic sample (same events, same order on every run); the join path is the conformed one of the DW
REPLAY_SQL = f"SELECT {_SELECT_COLUMNS}\n{_FROM_WHERE}\nORDER BY CRC32(f.estu_consecutivo), f.estu_consecutivo"
COUNT_SQL = f"SELECT COUNT(*)\n{_FROM_WHERE}"

INVALID_KINDS = ["bad_department", "score_out_of_range", "missing_field", "wrong_type",
                 "semantic_suspicious_flag", "malformed_json", "key_mismatch", "extra_field"]


# ------------------------------------------------------------------ event building
def build_event(row: dict, now: datetime | None = None) -> dict:
    """Maps one fact row (joined with its dimensions) to a contract v1 event dict."""
    now = now or datetime.now(timezone.utc)
    num = lambda v: None if v is None else float(v)  # noqa: E731 - DECIMAL -> float
    return {
        "event_id": contract.event_id_for(row["estu_consecutivo"], row["periodo"]),
        "event_type": settings.EVENT_TYPE,
        "event_version": settings.EVENT_VERSION,
        "event_ts": now.isoformat(timespec="milliseconds"),
        "cod_dane_depto": row["cod_dane_depto"],
        "estu_consecutivo": row["estu_consecutivo"],
        "periodo": row["periodo"],
        "cole_naturaleza": row["cole_naturaleza"],
        "cole_jornada": row["cole_jornada"],
        "cole_area_ubicacion": row["cole_area_ubicacion"],
        "estu_genero": row["estu_genero"],
        "punt_lectura_critica": num(row["punt_lectura_critica"]),
        "punt_matematicas": num(row["punt_matematicas"]),
        "punt_sociales_ciudadanas": num(row["punt_sociales_ciudadanas"]),
        "punt_c_naturales": num(row["punt_c_naturales"]),
        "punt_ingles": num(row["punt_ingles"]),
        "punt_global": num(row["punt_global"]),
        "puntaje_sospechoso": bool(row["puntaje_sospechoso"]),
    }


def corrupt(event: dict, kind: str) -> tuple[bytes, str]:
    """Returns (value_bytes, key) of a deliberately INVALID version of ``event``."""
    bad = dict(event)
    key = event["cod_dane_depto"]
    if kind == "bad_department":          # same defect injected in the 2D reliability test (code 999)
        bad["cod_dane_depto"], key = "999", "999"
    elif kind == "score_out_of_range":    # DQ-05: global score must be within [0, 500]
        bad["punt_global"] = 900.0
        bad["puntaje_sospechoso"] = False
    elif kind == "missing_field":         # required field absent
        bad.pop("punt_matematicas", None)
    elif kind == "wrong_type":            # data type violation
        bad["punt_ingles"] = "abc"
    elif kind == "semantic_suspicious_flag":  # DQ-07: flag inconsistent with the score
        bad["puntaje_sospechoso"] = True
        bad["punt_global"] = 250.0
    elif kind == "malformed_json":
        return b'{"event_id": "truncated-payload", "punt_global": ', key
    elif kind == "key_mismatch":          # message key is not the department of the body
        key = "05" if event["cod_dane_depto"] != "05" else "11"
    elif kind == "extra_field":           # the contract forbids unknown fields
        bad["campo_no_declarado"] = 1
    else:
        raise ValueError(kind)
    return json.dumps(bad, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), key


def serialize(event: dict) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# ------------------------------------------------------------------ source checks
def warn_if_no_men_coverage(conn, periodo: str) -> None:
    """M6/M7 need MEN indicators for the replayed year (the batch window is 2022-2024)."""
    anio = int(periodo[:4])
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT a.anio FROM fact_indicador_departamento i JOIN dim_anio a ON a.id_anio = i.id_anio "
                        "GROUP BY a.anio")
            years = sorted(int(r[0]) for r in cur.fetchall())
    except pymysql.err.ProgrammingError:
        log.warning("fact_indicador_departamento not found: run the Airflow DAG before the replay")
        return
    if anio not in years:
        log.warning("period %s: the DW has MEN indicators for %s only. Events will replay fine, but "
                    "M6/M7 (score vs dropout/repetition) will stay empty. Use a period from %d-%d.",
                    periodo, years or "no year", settings.MEN_FIRST_YEAR, settings.MEN_LAST_YEAR)


# ------------------------------------------------------------------------- run
class Replay:
    def __init__(self, args: argparse.Namespace) -> None:
        from confluent_kafka import Producer

        self.args = args
        self.running = True
        self.producer = Producer({
            "bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all",
            "linger.ms": 20, "compression.type": "lz4",
        })
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def _produce(self, key: str, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(self.args.topic, key=key.encode(), value=value, on_delivery=self._on_delivery)
                break
            except BufferError:           # local queue full (burst mode): serve callbacks and retry
                self.producer.poll(0.5)
        self.producer.poll(0)

    def run(self) -> int:
        a = self.args
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "running", False))
        conn = dw_db.get_connection()
        warn_if_no_men_coverage(conn, a.period)
        with conn.cursor() as cur:
            cur.execute(COUNT_SQL, (a.period, a.sample_pct))
            total = int(cur.fetchone()[0])
        if a.limit:
            total = min(total, a.limit)
        if total == 0:
            log.error("no rows for period %s (is the DW loaded?)", a.period)
            return 1

        n_invalid = min(a.inject_invalid, total)
        if n_invalid < a.inject_invalid:
            log.warning("--inject-invalid %d exceeds the %d events of the sample: injecting %d",
                        a.inject_invalid, total, n_invalid)
        invalid_at = {math.floor((i + 0.5) * total / n_invalid): INVALID_KINDS[i % len(INVALID_KINDS)]
                      for i in range(n_invalid)} if n_invalid else {}
        rng = random.Random(a.seed)
        dup_at = set(rng.sample(range(1, total), min(a.inject_duplicates, total - 1))) if a.inject_duplicates else set()

        log.info("=== REPLAY SIMULATION === historical records of fact_resultado_saber11, period %s, "
                 "sample %d%% -> %d events, topic '%s', %s", a.period, a.sample_pct, total, a.topic,
                 "burst (no pacing)" if a.burst else f"{a.rate:g} events/s")
        log.info("planned experiments: %d invalid, %d duplicates", len(invalid_at), len(dup_at))

        interval = 0.0 if a.burst else 1.0 / a.rate
        next_at = time.monotonic()
        sent = skipped = 0
        published_ids: deque[tuple[str, bytes]] = deque(maxlen=1000)  # pool for duplicate injection
        read_conn = dw_db.get_connection()
        try:
            with read_conn.cursor(pymysql.cursors.SSDictCursor) as cur:   # server-side cursor: constant memory
                cur.execute(REPLAY_SQL, (a.period, a.sample_pct))
                for i, row in enumerate(cur):
                    if not self.running or i >= total:
                        break
                    event = build_event(row)
                    try:
                        contract.parse_event(serialize(event))     # never publish a bad record by accident
                    except Exception as exc:  # noqa: BLE001
                        skipped += 1
                        log.warning("source row %s violates the contract and was NOT published: %s",
                                    row["estu_consecutivo"], contract.describe_error(exc))
                        continue
                    value = serialize(event)
                    self._produce(event["cod_dane_depto"], value)
                    sent += 1
                    published_ids.append((event["cod_dane_depto"], value))

                    if i in invalid_at:
                        bad_value, bad_key = corrupt(event, invalid_at[i])
                        self._produce(bad_key, bad_value)
                        log.info("injected INVALID event (%s) after record %d", invalid_at[i], i)
                    if i in dup_at and published_ids:
                        d_key, d_val = published_ids[rng.randrange(len(published_ids))]
                        self._produce(d_key, d_val)
                        log.info("injected DUPLICATE of an already published event after record %d", i)

                    if sent % a.log_every == 0:
                        log.info("published %d/%d (delivered %d)", sent, total, self.delivered)
                    if interval:
                        next_at += interval
                        delay = next_at - time.monotonic()
                        if delay > 0:
                            time.sleep(delay)
        finally:
            self.producer.flush(30)
            read_conn.close()
            conn.close()
        log.info("done: %d records published, %d skipped (contract), %d delivered, %d failed",
                 sent, skipped, self.delivered, self.failed)
        return 0 if self.failed == 0 else 2


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Replay of Saber 11 records to Kafka (SIMULATION of event arrival)")
    ap.add_argument("--period", required=True, help="period to replay, e.g. 2024-2 (MEN indicators exist for 2022-2024)")
    ap.add_argument("--sample-pct", type=int, default=1, help="deterministic sample, percent of the period (1-100)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = whole sample)")
    ap.add_argument("--rate", type=float, default=20.0, help="events per second")
    ap.add_argument("--burst", action="store_true", help="no pacing: publish as fast as possible (peak simulation)")
    ap.add_argument("--inject-invalid", type=int, default=0, help="publish N extra contract-violating events")
    ap.add_argument("--inject-duplicates", type=int, default=0, help="publish N extra exact duplicates")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--log-every", type=int, default=100)
    ap.add_argument("--bootstrap", default=settings.KAFKA_BOOTSTRAP_SERVERS)
    ap.add_argument("--topic", default=settings.TOPIC_RESULTS)
    args = ap.parse_args(argv)
    if not 1 <= args.sample_pct <= 100:
        ap.error("--sample-pct must be between 1 and 100")
    if args.rate <= 0:
        ap.error("--rate must be > 0")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return Replay(args).run()


if __name__ == "__main__":
    sys.exit(main())
