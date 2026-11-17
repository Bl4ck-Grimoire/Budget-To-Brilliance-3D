"""The Grafana dashboard is code: check it parses, does not overlap, and that EVERY panel query runs
with the read-only account Grafana will use, on data produced by the consumer."""
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pymysql
import pytest

from src import config
from src.streaming import producer, settings, store
from tests.test_streaming_db import NOW, replay_messages, run_in_batches

DASH = Path(__file__).resolve().parents[1] / "grafana" / "dashboards" / "streaming.json"


def load():
    return json.loads(DASH.read_text(encoding="utf-8"))


def test_dashboard_json_is_current():
    import importlib.util
    spec = importlib.util.spec_from_file_location("gen", DASH.parents[2] / "scripts" / "build_grafana_dashboard.py")
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    assert json.loads(json.dumps(gen.dashboard)) == load(), "run: python scripts/build_grafana_dashboard.py"


def test_panels_do_not_overlap_and_fit_the_grid():
    rects = [(p["gridPos"]["x"], p["gridPos"]["y"], p["gridPos"]["w"], p["gridPos"]["h"]) for p in load()["panels"]]
    for x, y, w, h in rects:
        assert 0 <= x and x + w <= 24
    for i, a in enumerate(rects):
        for b in rects[i + 1:]:
            apart = a[0] + a[2] <= b[0] or b[0] + b[2] <= a[0] or a[1] + a[3] <= b[1] or b[1] + b[3] <= a[1]
            assert apart, f"panels overlap: {a} {b}"


def test_at_least_three_streaming_metrics_are_displayed():
    titles = " ".join(p["title"] for p in load()["panels"])
    assert all(m in titles for m in ["M1", "M2", "M3", "M4", "M5", "M6", "M7"])


def test_every_panel_query_runs_with_the_readonly_account(dw):
    store.reset_streaming_tables(dw)
    store.refresh_reference(dw, NOW)
    msgs = replay_messages(dw)[:800]
    bad = [producer.corrupt({**__import__("tests.conftest", fromlist=["make_event"]).make_event()}, "bad_department")]
    from tests.conftest import FakeMsg
    msgs += [FakeMsg(v, k, partition=2, offset=i) for i, (v, k) in enumerate(bad)]
    # REAL clock on purpose: the latency panel compares against UTC_TIMESTAMP(), a fixed fake clock would make it flaky
    real_now = datetime.now(timezone.utc).replace(tzinfo=None)
    run_in_batches(dw, msgs, store.load_reference_keys(dw), now=real_now)

    store.ensure_grafana_user(dw)
    cfg = config.db_config()
    try:
        conn = pymysql.connect(host=cfg["host"], port=int(cfg["port"]), user=settings.GRAFANA_DB_USER,
                               password=settings.GRAFANA_DB_PASSWORD, database=cfg["dbname"], autocommit=True)
    except pymysql.err.OperationalError as exc:
        pytest.skip(f"cannot connect as {settings.GRAFANA_DB_USER}: {exc}")

    macros = [(r"\$__timeFilter\(\w+\)", "1=1"), (r"\$anio", "2024"), (r"\$bucket_seconds", "10")]
    empty = []
    try:
        for panel in load()["panels"]:
            for t in panel.get("targets", []):
                sql = t["rawSql"]
                for pattern, value in macros:
                    sql = re.sub(pattern, value, sql)
                assert "$" not in sql, f"unresolved variable in panel '{panel['title']}'"
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()
                if not rows or all(v is None for r in rows for v in r):
                    empty.append(panel["title"])
    finally:
        conn.close()                      # never leave a connection open: it would block later TRUNCATEs
    assert not empty, f"panels with no data after processing events: {empty}"
