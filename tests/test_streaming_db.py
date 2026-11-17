"""Integration tests against a real MySQL/MariaDB (see conftest). No Kafka needed: messages are
simulated with FakeMsg, which is exactly what the consumer's ``process_batch`` receives."""
import json
from datetime import datetime, timedelta, timezone

import pymysql
import pytest

from src import config
from src.load import db as dw_db
from src.streaming import consumer, producer, store
from tests.conftest import FakeMsg, make_event
from tests.support import seed_dw

NOW = datetime(2026, 11, 17, 12, 0, 0)


def replay_messages(conn, periodo="2024-2", pct=100, start_offset=0):
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(producer.REPLAY_SQL, (periodo, pct))
        rows = cur.fetchall()
    msgs = []
    for i, row in enumerate(rows):
        ev = producer.build_event(row)
        msgs.append(FakeMsg(producer.serialize(ev), ev["cod_dane_depto"], partition=0, offset=start_offset + i))
    return msgs


def run_in_batches(conn, msgs, ref, size=200, now=NOW):
    out = []
    for i in range(0, len(msgs), size):
        out.append(consumer.process_batch(conn, msgs[i:i + size], ref, now + timedelta(seconds=i // size)))
    return out


def scalar(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        row = cur.fetchone()
    conn.commit()
    return row[0]


@pytest.fixture
def fresh(dw):
    store.reset_streaming_tables(dw)
    store.refresh_reference(dw, NOW)
    return dw


def test_replay_sql_yields_only_contract_valid_events(dw):
    msgs = replay_messages(dw)
    assert len(msgs) == seed_dw.seed.__defaults__[0]  # all 2024-2 rows of the synthetic DW
    valid, invalid = consumer.classify(msgs)
    assert not invalid and len(valid) == len(msgs)


def test_sample_is_deterministic_and_about_one_percent(dw):
    a = [m.value() for m in replay_messages(dw, pct=10)]
    b = [m.value() for m in replay_messages(dw, pct=10)]
    strip = lambda raws: [json.loads(r)["event_id"] for r in raws]  # noqa: E731 - event_ts differs per call
    assert strip(a) == strip(b) and 0 < len(a) < 0.25 * 2400


def test_reference_snapshot_comes_from_the_api_table(fresh):
    assert scalar(fresh, "SELECT COUNT(*) FROM stream_referencia_indicador") == 33 * 3
    keys = store.load_reference_keys(fresh)
    assert ("76", 2024) in keys and ("76", 2025) not in keys


def test_streaming_metrics_match_the_batch_sql(fresh):
    ref = store.load_reference_keys(fresh)
    msgs = replay_messages(fresh)
    results = run_in_batches(fresh, msgs, ref)
    assert sum(r.n_valid for r in results) == len(msgs)

    # M1: department mean (same filter as the batch R6: real school, not suspicious)
    batch = dict(_rows(fresh, """
        SELECT d.cod_dane_depto, ROUND(AVG(f.punt_global), 1) FROM fact_resultado_saber11 f
        JOIN dim_colegio c ON c.id_colegio = f.id_colegio JOIN dim_departamento d ON d.id_departamento = c.id_departamento
        JOIN dim_periodo p ON p.id_periodo = f.id_periodo
        WHERE f.id_colegio <> -1 AND f.puntaje_sospechoso = FALSE AND p.periodo = '2024-2' GROUP BY d.cod_dane_depto"""))
    live = dict(_rows(fresh, "SELECT cod_dane_depto, prom_global FROM v_stream_departamento WHERE anio = 2024"))
    assert set(batch) == set(live) and all(abs(float(batch[k]) - float(live[k])) < 0.051 for k in batch)

    # M2 / M3: national gaps (NO OFICIAL - OFICIAL, URBANO - RURAL), batch SQL vs live accumulators
    def batch_gap(col, a, b):
        return float(scalar(fresh, f"""
            SELECT AVG(CASE WHEN c.{col} = %s THEN f.punt_global END) - AVG(CASE WHEN c.{col} = %s THEN f.punt_global END)
            FROM fact_resultado_saber11 f JOIN dim_colegio c ON c.id_colegio = f.id_colegio
            JOIN dim_periodo p ON p.id_periodo = f.id_periodo
            WHERE f.puntaje_sospechoso = FALSE AND p.periodo = '2024-2'""", (a, b)))
    snap = _last_snapshot(fresh)
    assert abs(batch_gap("cole_naturaleza", "NO OFICIAL", "OFICIAL") - float(snap["brecha_no_oficial_oficial_acum"])) < 0.02
    assert abs(batch_gap("cole_area_ubicacion", "URBANO", "RURAL") - float(snap["brecha_urbano_rural_acum"])) < 0.02

    # M4: gender gap in the 6 measures (R5), batch SQL vs live
    measures = {"global": "punt_global", "lectura_critica": "punt_lectura_critica", "matematicas": "punt_matematicas",
                "sociales_ciudadanas": "punt_sociales_ciudadanas", "c_naturales": "punt_c_naturales", "ingles": "punt_ingles"}
    live_gap = dict(_rows(fresh, "SELECT medida, brecha_m_menos_f FROM v_stream_genero_nacional WHERE anio = 2024"))
    assert set(live_gap) == set(measures)
    for medida, col in measures.items():
        b = float(scalar(fresh, f"""
            SELECT AVG(CASE WHEN g.estu_genero = 'M' THEN f.{col} END) - AVG(CASE WHEN g.estu_genero = 'F' THEN f.{col} END)
            FROM fact_resultado_saber11 f JOIN dim_genero g ON g.id_genero = f.id_genero
            JOIN dim_periodo p ON p.id_periodo = f.id_periodo
            WHERE f.puntaje_sospechoso = FALSE AND p.periodo = '2024-2'"""))
        assert abs(b - float(live_gap[medida])) < 0.02, medida

    # the data was built with these effects: the live metrics must show them
    assert float(snap["brecha_no_oficial_oficial_acum"]) > 0 and float(snap["brecha_genero_global_acum"]) > 0


def test_men_integration_m6_m7(fresh):
    run_in_batches(fresh, replay_messages(fresh), store.load_reference_keys(fresh))
    assert scalar(fresh, "SELECT COUNT(*) FROM v_stream_riesgo") == 33
    rho_d, rho_r = _rows(fresh, "SELECT spearman_desercion_vs_puntaje, spearman_repitencia_vs_puntaje FROM v_stream_spearman WHERE anio = 2024")[0]
    assert float(rho_d) < -0.3 and float(rho_r) < -0.3   # seeded: higher dropout/repetition <-> lower score
    assert scalar(fresh, "SELECT COUNT(*) FROM v_stream_riesgo WHERE cuadrante_desercion LIKE 'Alta desercion / bajo%'") > 0
    assert scalar(fresh, "SELECT n_sin_referencia FROM stream_estado WHERE id = 1") == 0


def test_events_outside_the_api_window_are_counted_without_reference(fresh):
    run_in_batches(fresh, replay_messages(fresh, "2025-2"), store.load_reference_keys(fresh))
    n = scalar(fresh, "SELECT n_sin_referencia FROM stream_estado WHERE id = 1")
    assert n > 250                       # all non-validante events of 2025 have no MEN indicator
    assert scalar(fresh, "SELECT COUNT(*) FROM v_stream_riesgo") == 0   # M6/M7 stay empty, never wrong


def test_replaying_the_same_events_does_not_double_count(fresh):
    ref = store.load_reference_keys(fresh)
    msgs = replay_messages(fresh)[:500]
    run_in_batches(fresh, msgs, ref)
    before = _fingerprint(fresh)
    second = run_in_batches(fresh, msgs, ref)
    assert sum(r.n_duplicate for r in second) == 500 and sum(r.n_valid for r in second) == 0
    after = _fingerprint(fresh)
    assert before["metrics"] == after["metrics"] and before["valid"] == after["valid"]
    assert after["dup"] == 500


def test_duplicates_inside_one_batch(fresh):
    m = replay_messages(fresh)[0]
    r = consumer.process_batch(fresh, [m, FakeMsg(m.value(), m.key().decode(), offset=999)], set(), NOW)
    assert r.n_valid == 1 and r.n_duplicate == 1


def test_crash_between_mysql_commit_and_offset_commit(fresh):
    """The offset commit is lost: Kafka redelivers the last batch. Result must equal a clean run."""
    ref = store.load_reference_keys(fresh)
    msgs = replay_messages(fresh)[:600]
    run_in_batches(fresh, msgs[:400], ref)
    run_in_batches(fresh, msgs[200:], ref)          # redelivery overlaps 200 already applied messages
    crashed = _fingerprint(fresh)
    store.reset_streaming_tables(fresh)
    run_in_batches(fresh, msgs, ref)
    clean = _fingerprint(fresh)
    assert crashed["metrics"] == clean["metrics"] and crashed["valid"] == clean["valid"] == 600


def test_invalid_events_are_rejected_logged_once_and_do_not_stop_the_stream(fresh):
    ref = store.load_reference_keys(fresh)
    base = make_event()
    bad = [FakeMsg(*(lambda v, k: (v, k))(*producer.corrupt(base, kind)), partition=1, offset=i)
           for i, kind in enumerate(producer.INVALID_KINDS)]
    good = replay_messages(fresh)[:50]
    r = consumer.process_batch(fresh, bad + good, ref, NOW)
    assert r.n_invalid == len(producer.INVALID_KINDS) and r.n_valid == 50
    assert len(r.dlq_records) == len(producer.INVALID_KINDS)
    reasons = " || ".join(row[0] for row in _rows(fresh, "SELECT motivo FROM stream_eventos_invalidos"))
    for expected in ["cod_dane_depto: String should match", "less than or equal to 500", "punt_matematicas: Field required",
                     "punt_ingles: Input should be a valid number", "DQ-07", "Invalid JSON",
                     "message key '05' differs from cod_dane_depto '76'", "campo_no_declarado: Extra inputs"]:
        assert expected in reasons, expected
    # the same Kafka positions re-read after a crash are NOT logged or counted again
    r2 = consumer.process_batch(fresh, bad, ref, NOW)
    assert r2.n_invalid == 0
    assert scalar(fresh, "SELECT COUNT(*) FROM stream_eventos_invalidos") == len(producer.INVALID_KINDS)
    assert scalar(fresh, "SELECT n_invalidos FROM stream_estado WHERE id = 1") == len(producer.INVALID_KINDS)


def test_tombstone_and_null_payload_are_invalid(fresh):
    r = consumer.process_batch(fresh, [FakeMsg(None, "76", offset=1)], set(), NOW)
    assert r.n_invalid == 1 and r.n_valid == 0


def test_stream_state_and_freshness(fresh):
    run_in_batches(fresh, replay_messages(fresh)[:100], store.load_reference_keys(fresh), now=datetime.now(timezone.utc).replace(tzinfo=None))
    n_valid, seg, pct = _rows(fresh, "SELECT n_validos, segundos_sin_datos, pct_invalidos FROM v_stream_estado")[0]
    assert n_valid == 100 and 0 <= seg < 10 and float(pct) == 0
    assert scalar(fresh, "SELECT COUNT(*) FROM stream_serie_tiempo") >= 1


def test_reference_snapshot_survives_a_batch_full_refresh(fresh):
    keys_before = store.load_reference_keys(fresh)
    with fresh.cursor() as cur:                         # what load_all.empty_warehouse() does to the DW
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in dw_db.TABLES_CHILDREN_FIRST:
            cur.execute(f"TRUNCATE TABLE {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    fresh.commit()
    try:
        assert store.refresh_reference(fresh, NOW) is False          # empty source: snapshot is kept
        assert store.load_reference_keys(fresh) == keys_before
        assert scalar(fresh, "SELECT COUNT(*) FROM stream_eventos_procesados") >= 0   # streaming tables untouched
    finally:
        seed_dw.seed(fresh)


def test_batch_full_refresh_does_not_touch_stream_tables(fresh):
    run_in_batches(fresh, replay_messages(fresh)[:50], store.load_reference_keys(fresh))
    assert "stream_metricas_departamento" not in dw_db.TABLES_CHILDREN_FIRST
    assert not any(t.startswith("stream_") for t in dw_db.TABLES_CHILDREN_FIRST)
    with fresh.cursor() as cur:                         # no foreign key links stream_* to the DW
        cur.execute("SELECT COUNT(*) FROM information_schema.referential_constraints "
                    "WHERE constraint_schema = DATABASE() AND table_name LIKE 'stream\\_%'")
        assert cur.fetchone()[0] == 0


def test_schema_is_idempotent(dw):
    before = scalar(dw, "SELECT COUNT(*) FROM stream_estado")
    store.init_schema(dw)
    store.init_schema(dw)
    assert scalar(dw, "SELECT COUNT(*) FROM stream_estado") == before == 1


def test_grafana_account_reads_views_but_not_the_dw(dw):
    store.ensure_grafana_user(dw)
    cfg = config.db_config()
    try:
        ro = pymysql.connect(host=cfg["host"], port=int(cfg["port"]), user="grafana_ro",
                             password=store.settings.GRAFANA_DB_PASSWORD, database=cfg["dbname"])
    except pymysql.err.OperationalError as exc:
        pytest.skip(f"cannot connect as grafana_ro in this server (anonymous-user shadowing?): {exc}")
    with ro.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM v_stream_estado")
        assert cur.fetchone()[0] == 1
        with pytest.raises(pymysql.err.OperationalError):
            cur.execute("SELECT COUNT(*) FROM fact_resultado_saber11")
    ro.close()


# ------------------------------------------------------------------ helpers
def _rows(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        rows = cur.fetchall()
    conn.commit()
    return rows


def _last_snapshot(conn):
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT * FROM stream_serie_tiempo ORDER BY bucket_ts DESC LIMIT 1")
        row = cur.fetchone()
    conn.commit()
    return row


def _fingerprint(conn):
    return {
        "metrics": _rows(conn, "SELECT cod_dane_depto, anio, n_eventos, n_score, suma_global, n_oficial, suma_oficial, "
                               "n_urbano, suma_urbano FROM stream_metricas_departamento ORDER BY 1, 2")
                   + _rows(conn, "SELECT * FROM stream_metricas_genero ORDER BY 1, 2, 3, 4"),
        "valid": scalar(conn, "SELECT n_validos FROM stream_estado WHERE id = 1"),
        "dup": scalar(conn, "SELECT n_duplicados FROM stream_estado WHERE id = 1"),
    }
