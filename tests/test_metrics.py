import json
from datetime import datetime, timezone

import pytest

from src.streaming import contract, metrics, settings
from tests.conftest import make_event

NOW = datetime(2026, 11, 17, 12, 0, 5)


def ev(**over):
    return contract.parse_event(json.dumps(make_event(**over)))


def test_counts_and_sums_for_one_event():
    agg = metrics.aggregate([ev()], {("76", 2024)}, NOW)
    acc = agg.dept[("76", 2024)]
    assert acc["n_eventos"] == acc["n_score"] == acc["n_oficial"] == acc["n_urbano"] == 1
    assert acc["suma_global"] == acc["suma_oficial"] == acc["suma_urbano"] == 260.0
    assert agg.n_sin_referencia == 0


def test_suspicious_events_never_enter_scores():
    s = ev(punt_global=0.0, puntaje_sospechoso=True)
    agg = metrics.aggregate([s], {("76", 2024)}, NOW)
    acc = agg.dept[("76", 2024)]
    assert acc["n_eventos"] == acc["n_sospechosos"] == 1 and acc["n_score"] == 0 and acc["suma_global"] == 0
    assert not agg.gender


def test_gender_gap_covers_global_and_the_five_subjects():
    agg = metrics.aggregate([ev(estu_genero="F"), ev(estu_genero="M", estu_consecutivo="B")], {("76", 2024)}, NOW)
    measures = {k[2] for k in agg.gender}
    assert measures == {"global", "lectura_critica", "matematicas", "sociales_ciudadanas", "c_naturales", "ingles"}
    assert len(agg.gender) == 6 * 2


def test_no_informado_gender_and_no_aplica_are_left_out_of_gaps():
    e = ev(estu_genero="No informado", cole_naturaleza="OFICIAL")
    agg = metrics.aggregate([e], {("76", 2024)}, NOW)
    assert not agg.gender
    validante = ev(cod_dane_depto="00", cole_naturaleza="No aplica", cole_jornada="No aplica", cole_area_ubicacion="No aplica")
    acc = metrics.aggregate([validante], set(), NOW).dept[("00", 2024)]
    assert acc["n_score"] == 1 and acc["n_oficial"] == acc["n_no_oficial"] == acc["n_urbano"] == acc["n_rural"] == 0


def test_event_without_men_reference_is_counted_but_validantes_are_not():
    agg = metrics.aggregate([ev(periodo="2025-2")], {("76", 2024)}, NOW)
    assert agg.n_sin_referencia == 1
    validante = ev(cod_dane_depto="00", cole_naturaleza="No aplica", cole_jornada="No aplica", cole_area_ubicacion="No aplica")
    assert metrics.aggregate([validante], set(), NOW).n_sin_referencia == 0


def test_latency_is_non_negative():
    future = ev(event_ts=datetime(2026, 11, 17, 12, 0, 59, tzinfo=timezone.utc).isoformat())
    assert metrics.aggregate([future], set(), NOW).sum_latency_ms == 0.0


def test_bucket_alignment():
    b = metrics.bucket_of(datetime(2026, 11, 17, 12, 0, 17), 10)
    assert b == datetime(2026, 11, 17, 12, 0, 10)
