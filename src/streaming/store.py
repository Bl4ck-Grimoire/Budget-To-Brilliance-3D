"""MySQL persistence for the streaming component.

Everything that touches the database lives here, so ``metrics.py`` stays pure (easy to unit test)
and ``consumer.py`` only orchestrates. The connection helpers are REUSED from the batch pipeline
(``src.load.db``): same ``DATABASE_URL``, same driver, same DDL runner.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Iterable

import pymysql

from src import config
from src.load import db as dw_db
from src.streaming import settings
from src.streaming.metrics import DEPT_COUNTERS, BatchAggregate

log = logging.getLogger(__name__)

STREAMING_DDL = config.SQL_DIR / "schema" / "create_streaming.sql"
STREAM_TABLES_RESET_ORDER = [
    "stream_eventos_procesados", "stream_eventos_invalidos", "stream_metricas_departamento",
    "stream_metricas_genero", "stream_serie_tiempo",
]
_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


# ------------------------------------------------------------------ setup
def init_schema(conn) -> None:
    """Creates the streaming tables and views (idempotent)."""
    dw_db.create_schema(conn, STREAMING_DDL)


def ensure_grafana_user(conn) -> None:
    """Read-only account for Grafana: SELECT on the streaming tables and views only.
    The dashboard never connects as root and never sees the DW tables."""
    user, password = settings.GRAFANA_DB_USER, settings.GRAFANA_DB_PASSWORD
    dbname = config.db_config()["dbname"]
    with conn.cursor() as cur:
        cur.execute("CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s", (user, password))
        cur.execute("ALTER USER %s@'%%' IDENTIFIED BY %s", (user, password))  # keeps the password in sync with .env
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND (table_name LIKE 'stream\\_%%' OR table_name LIKE 'v\\_stream\\_%%')",
            (dbname,),
        )
        names = [r[0] for r in cur.fetchall()]
        for name in names:
            if not _IDENT.match(name):
                raise ValueError(f"unexpected object name: {name}")
            cur.execute(f"GRANT SELECT ON `{dbname}`.`{name}` TO %s@'%%'", (user,))
        cur.execute("FLUSH PRIVILEGES")
    conn.commit()
    log.info("[STREAM-INIT] read-only user '%s' granted SELECT on %d objects", user, len(names))


def reset_streaming_tables(conn) -> None:
    """Empties the accumulators for a clean demo. Does NOT touch the Data Warehouse tables."""
    with conn.cursor() as cur:
        for table in STREAM_TABLES_RESET_ORDER:
            cur.execute(f"TRUNCATE TABLE {table}")
        cur.execute("UPDATE stream_estado SET n_validos = 0, n_invalidos = 0, n_duplicados = 0, "
                    "n_sin_referencia = 0, last_event_ts = NULL, last_processed_at = NULL WHERE id = 1")
    conn.commit()
    log.info("[STREAM-INIT] streaming tables emptied (Data Warehouse untouched)")


# ------------------------------------------------------- MEN reference (API)
_REFERENCE_SQL_DEPTS = "SELECT cod_dane_depto, nombre_departamento FROM dim_departamento"
_REFERENCE_SQL_INDICATORS = (
    "SELECT d.cod_dane_depto, a.anio, i.desercion_media, i.repitencia_secundaria "
    "FROM fact_indicador_departamento i "
    "JOIN dim_departamento d ON d.id_departamento = i.id_departamento "
    "JOIN dim_anio a ON a.id_anio = i.id_anio"
)


def refresh_reference(conn, now: datetime) -> bool:
    """Copies the MEN indicators loaded by the Airflow API branch from the DW into the streaming
    snapshot. Returns True if the snapshot was replaced.

    The batch DAG does a full refresh (it empties the DW before loading). If the consumer reads at
    that moment it finds an empty source: the previous snapshot is KEPT in that case.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(_REFERENCE_SQL_DEPTS)
            depts = cur.fetchall()
            cur.execute(_REFERENCE_SQL_INDICATORS)
            indicators = cur.fetchall()
        conn.commit()
    except pymysql.err.ProgrammingError as exc:  # DW tables do not exist yet (DAG never ran)
        conn.rollback()
        log.warning("[REFERENCE] Data Warehouse tables not available yet (%s); keeping current snapshot", exc.args[-1])
        return False
    if not depts or not indicators:
        log.warning("[REFERENCE] DW source is empty (batch full refresh in progress?); keeping current snapshot")
        return False
    with conn.cursor() as cur:
        cur.execute("DELETE FROM stream_referencia_indicador")
        cur.execute("DELETE FROM stream_referencia_departamento")
        cur.executemany(
            "INSERT INTO stream_referencia_departamento (cod_dane_depto, nombre_departamento, loaded_at) VALUES (%s, %s, %s)",
            [(c, n, now) for c, n in depts],
        )
        cur.executemany(
            "INSERT INTO stream_referencia_indicador (cod_dane_depto, anio, desercion_media, repitencia_secundaria, loaded_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            [(c, int(a), d, r, now) for c, a, d, r in indicators],
        )
    conn.commit()
    log.info("[REFERENCE] MEN snapshot refreshed: %d departments, %d department-year indicators", len(depts), len(indicators))
    return True


def load_reference_keys(conn) -> set[tuple[str, int]]:
    """(cod_dane_depto, anio) pairs that have MEN indicators: used to count events without API context."""
    with conn.cursor() as cur:
        cur.execute("SELECT cod_dane_depto, anio FROM stream_referencia_indicador")
        rows = cur.fetchall()
    conn.commit()
    return {(c, int(a)) for c, a in rows}


# ---------------------------------------------------------------- idempotency
def existing_event_ids(cur, event_ids: list[str]) -> set[str]:
    if not event_ids:
        return set()
    placeholders = ", ".join(["%s"] * len(event_ids))
    cur.execute(f"SELECT event_id FROM stream_eventos_procesados WHERE event_id IN ({placeholders})", event_ids)
    return {r[0] for r in cur.fetchall()}


# Parameter order: key columns + updated_at + counters (insert), then updated_at + counters (update).
_DEPT_UPSERT = (
    "INSERT INTO stream_metricas_departamento (cod_dane_depto, anio, updated_at, "
    + ", ".join(DEPT_COUNTERS) + ") VALUES (%s, %s, %s, " + ", ".join(["%s"] * len(DEPT_COUNTERS)) + ") "
    "ON DUPLICATE KEY UPDATE updated_at = %s, " + ", ".join(f"{c} = {c} + %s" for c in DEPT_COUNTERS)
)
_GENDER_UPSERT = (
    "INSERT INTO stream_metricas_genero (cod_dane_depto, anio, medida, estu_genero, n, suma) "
    "VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE n = n + %s, suma = suma + %s"
)


def apply_aggregate(cur, agg: BatchAggregate, new_event_ids: Iterable[str], now: datetime) -> None:
    """Applies one micro-batch to the accumulators. The caller owns the transaction."""
    cur.executemany(
        "INSERT INTO stream_eventos_procesados (event_id, processed_at) VALUES (%s, %s)",
        [(eid, now) for eid in new_event_ids],
    )
    dept_rows = []
    for (cod, anio), c in agg.dept.items():
        vals = [round(c[k], 1) if k.startswith("suma_") else int(c[k]) for k in DEPT_COUNTERS]
        dept_rows.append((cod, anio, now, *vals, now, *vals))
    if dept_rows:
        # execute() per row, NOT executemany(): PyMySQL cannot batch an INSERT whose ON DUPLICATE KEY UPDATE
        # part contains placeholders. A micro-batch touches few rows (departments x year), so this is cheap.
        for row in dept_rows:
            cur.execute(_DEPT_UPSERT, row)
    gender_rows = [
        (cod, anio, medida, g, int(n), round(s, 1), int(n), round(s, 1))
        for (cod, anio, medida, g), (n, s) in agg.gender.items()
    ]
    if gender_rows:
        for row in gender_rows:
            cur.execute(_GENDER_UPSERT, row)


def log_invalid(cur, rows: list[dict], now: datetime) -> list[dict]:
    """Stores rejected events. Returns only those that were NEW (a re-read after a crash is not counted twice)."""
    inserted = []
    for r in rows:
        cur.execute(
            "INSERT IGNORE INTO stream_eventos_invalidos (kafka_partition, kafka_offset, kafka_key, motivo, payload, recibido_en) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (r["partition"], r["offset"], (r["key"] or "")[:64], r["motivo"][:600], r["payload"], now),
        )
        if cur.rowcount == 1:
            inserted.append(r)
    return inserted


def update_state(cur, *, n_valid: int, n_invalid: int, n_dup: int, n_sin_ref: int,
                 last_event_ts: datetime | None, now: datetime) -> None:
    cur.execute(
        "UPDATE stream_estado SET n_validos = n_validos + %s, n_invalidos = n_invalidos + %s, "
        "n_duplicados = n_duplicados + %s, n_sin_referencia = n_sin_referencia + %s, "
        "last_event_ts = COALESCE(%s, last_event_ts), last_processed_at = %s WHERE id = 1",
        (n_valid, n_invalid, n_dup, n_sin_ref, last_event_ts, now),
    )


def national_snapshot(cur) -> dict[str, float | None]:
    """Cumulative national figures (all departments, all years seen) for the time series."""
    cur.execute(
        "SELECT SUM(suma_global), SUM(n_score), SUM(suma_oficial), SUM(n_oficial), SUM(suma_no_oficial), "
        "SUM(n_no_oficial), SUM(suma_urbano), SUM(n_urbano), SUM(suma_rural), SUM(n_rural) "
        "FROM stream_metricas_departamento"
    )
    s_g, n_g, s_of, n_of, s_no, n_no, s_u, n_u, s_r, n_r = [float(x) if x is not None else 0.0 for x in cur.fetchone()]
    cur.execute("SELECT estu_genero, SUM(n), SUM(suma) FROM stream_metricas_genero WHERE medida = 'global' GROUP BY estu_genero")
    gender = {g: (float(n), float(s)) for g, n, s in cur.fetchall()}

    def mean(s: float, n: float) -> float | None:
        return s / n if n else None

    def gap(a: float | None, b: float | None) -> float | None:
        return round(a - b, 2) if a is not None and b is not None else None

    mean_m = mean(gender["M"][1], gender["M"][0]) if "M" in gender else None  # (sum, n)
    mean_f = mean(gender["F"][1], gender["F"][0]) if "F" in gender else None
    return {
        "prom_global": round(mean(s_g, n_g), 2) if n_g else None,
        "brecha_no_oficial_oficial": gap(mean(s_no, n_no), mean(s_of, n_of)),
        "brecha_urbano_rural": gap(mean(s_u, n_u), mean(s_r, n_r)),
        "brecha_genero_global": gap(mean_m, mean_f),
    }


def upsert_time_bucket(cur, bucket: datetime, *, n_valid: int, n_invalid: int, n_dup: int, n_susp: int,
                       sum_latency_ms: float, n_latency: int, snapshot: dict) -> None:
    cur.execute(
        "INSERT INTO stream_serie_tiempo (bucket_ts, n_validos, n_invalidos, n_duplicados, n_sospechosos, "
        "suma_latencia_ms, n_latencia, prom_global_acum, brecha_no_oficial_oficial_acum, brecha_urbano_rural_acum, "
        "brecha_genero_global_acum) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE n_validos = n_validos + %s, n_invalidos = n_invalidos + %s, "
        "n_duplicados = n_duplicados + %s, n_sospechosos = n_sospechosos + %s, "
        "suma_latencia_ms = suma_latencia_ms + %s, n_latencia = n_latencia + %s, "
        "prom_global_acum = %s, brecha_no_oficial_oficial_acum = %s, brecha_urbano_rural_acum = %s, "
        "brecha_genero_global_acum = %s",
        (bucket, n_valid, n_invalid, n_dup, n_susp, sum_latency_ms, n_latency,
         snapshot["prom_global"], snapshot["brecha_no_oficial_oficial"], snapshot["brecha_urbano_rural"],
         snapshot["brecha_genero_global"],
         n_valid, n_invalid, n_dup, n_susp, sum_latency_ms, n_latency,
         snapshot["prom_global"], snapshot["brecha_no_oficial_oficial"], snapshot["brecha_urbano_rural"],
         snapshot["brecha_genero_global"]),
    )
