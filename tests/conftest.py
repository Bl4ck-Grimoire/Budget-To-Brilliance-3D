"""Shared fixtures. Database tests need a THROWAWAY MySQL/MariaDB: set TEST_DATABASE_URL, e.g.
   TEST_DATABASE_URL=mysql+pymysql://root:changeme@127.0.0.1:3307/icfes_dw_test
They are skipped otherwise. The tests CREATE and FILL that database with synthetic data, so never
point it at the real Data Warehouse."""
import os
from datetime import datetime, timezone

import pytest

from src import config
from src.streaming import contract, settings


class FakeMsg:
    """Duck-typed confluent_kafka.Message."""

    def __init__(self, value, key, partition=0, offset=0, topic="saber11.resultados.v1"):
        self._v, self._k, self._p, self._o, self._t = value, key, partition, offset, topic

    def value(self): return self._v
    def key(self): return None if self._k is None else self._k.encode()
    def partition(self): return self._p
    def offset(self): return self._o
    def topic(self): return self._t
    def error(self): return None


def make_event(**over) -> dict:
    ev = {
        "event_id": contract.event_id_for("SB11TEST0000001", "2024-2"),
        "event_type": settings.EVENT_TYPE, "event_version": 1,
        "event_ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "cod_dane_depto": "76", "estu_consecutivo": "SB11TEST0000001", "periodo": "2024-2",
        "cole_naturaleza": "OFICIAL", "cole_jornada": "COMPLETA", "cole_area_ubicacion": "URBANO", "estu_genero": "F",
        "punt_lectura_critica": 55.0, "punt_matematicas": 52.0, "punt_sociales_ciudadanas": 50.0,
        "punt_c_naturales": 51.0, "punt_ingles": 49.0, "punt_global": 260.0, "puntaje_sospechoso": False,
    }
    ev.update(over)
    return ev


@pytest.fixture
def event():
    return make_event


@pytest.fixture(scope="module")
def dw():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set (database tests need a throwaway MySQL/MariaDB)")
    old = config.DATABASE_URL
    config.DATABASE_URL = url
    os.environ.pop("MYSQL_DATABASE", None)
    from src.load import db as dw_db
    from src.streaming import store
    from tests.support import seed_dw

    dw_db.ensure_database_exists()
    conn = dw_db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SET SESSION lock_wait_timeout = 15")  # a forgotten lock must fail the test, not hang it
    dw_db.create_schema(conn)
    store.init_schema(conn)
    seed_dw.seed(conn)
    store.reset_streaming_tables(conn)
    yield conn
    conn.close()
    config.DATABASE_URL = old
