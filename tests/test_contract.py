import json

import pytest

from src.streaming import contract
from src.streaming.producer import INVALID_KINDS, corrupt
from tests.conftest import make_event


def raw(ev: dict) -> bytes:
    return json.dumps(ev).encode()


def test_valid_event_passes():
    ev = contract.parse_event(raw(make_event()))
    assert ev.anio == 2024 and ev.cod_dane_depto == "76"


def test_integer_scores_are_accepted_in_strict_mode():
    contract.parse_event(raw(make_event(punt_matematicas=52, punt_global=260)))


def test_event_id_is_deterministic():
    a = contract.event_id_for("SB11X", "2024-2")
    assert a == contract.event_id_for("SB11X", "2024-2") != contract.event_id_for("SB11X", "2024-1")


def test_no_is_simulation_field_in_contract():
    assert "is_simulation" not in contract.Saber11ResultEvent.model_fields
    with pytest.raises(Exception):  # extra fields are forbidden
        contract.parse_event(raw(make_event(is_simulation=True)))


@pytest.mark.parametrize("field", ["punt_global", "punt_matematicas", "punt_lectura_critica",
                                   "punt_sociales_ciudadanas", "punt_c_naturales", "punt_ingles"])
def test_all_scores_are_required(field):
    ev = make_event()
    ev.pop(field)
    with pytest.raises(Exception):
        contract.parse_event(raw(ev))


@pytest.mark.parametrize("field", ["punt_matematicas", "punt_ingles"])
def test_null_score_is_rejected(field):
    with pytest.raises(Exception):
        contract.parse_event(raw(make_event(**{field: None})))


@pytest.mark.parametrize("over", [
    {"cod_dane_depto": "999"},                                   # not 2 digits
    {"cod_dane_depto": "42"},                                    # 2 digits but not a DANE code
    {"punt_global": 501.0},                                      # DQ-05
    {"punt_matematicas": 100.5},                                 # DQ-05
    {"punt_ingles": "abc"},                                      # wrong type
    {"cole_naturaleza": "PRIVADO"},                              # closed domain
    {"estu_genero": "X"},
    {"periodo": "2024-3"},
    {"event_type": "other"},
    {"event_ts": "2024-01-01T10:00:00.000"},                     # no timezone
    {"puntaje_sospechoso": True},                                # DQ-07 inconsistent with punt_global 260
    {"cole_naturaleza": "No aplica"},                            # DQ-11: no school but dept 76
])
def test_invalid_events_are_rejected(over):
    with pytest.raises(Exception):
        contract.parse_event(raw(make_event(**over)))


def test_suspicious_zero_score_is_valid():
    contract.parse_event(raw(make_event(punt_global=0.0, puntaje_sospechoso=True)))


def test_validante_must_live_in_department_00():
    contract.parse_event(raw(make_event(cod_dane_depto="00", cole_naturaleza="No aplica",
                                        cole_jornada="No aplica", cole_area_ubicacion="No aplica")))


@pytest.mark.parametrize("kind", [k for k in INVALID_KINDS if k != "key_mismatch"])
def test_every_injected_kind_violates_the_contract(kind):
    value, _key = corrupt(make_event(), kind)
    with pytest.raises(Exception):
        contract.parse_event(value)


def test_key_mismatch_has_a_valid_body_and_is_caught_by_the_consumer_key_rule():
    # the body is a valid event: only the Kafka key is wrong (checked in consumer.classify, see test_consumer_db)
    value, key = corrupt(make_event(), "key_mismatch")
    assert contract.parse_event(value).cod_dane_depto == "76" and key != "76"


def test_exported_schema_is_current(tmp_path):
    path = contract.export_schema(tmp_path / "s.json")
    fresh = json.loads(path.read_text())
    committed = json.loads(contract.SCHEMA_PATH.read_text())
    assert fresh == committed, "run: python -m src.streaming.contract --export"
