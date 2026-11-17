"""Event contract v1 for ``saber11.resultados.v1``.

One event = one Saber 11 exam attempt (the grain of ``fact_resultado_saber11``), published by the
replay producer and validated by the consumer. Pydantic is the single source of truth; the JSON
Schema in ``contracts/`` is generated from it (``python -m src.streaming.contract --export``).

Contract elements required by the assignment:
  * required fields and data types  -> the model below (extra fields are rejected)
  * event identifier                -> ``event_id`` (UUIDv5 of ``estu_consecutivo|periodo``)
  * timestamp                       -> ``event_ts`` (UTC publication time)
  * event key                       -> ``cod_dane_depto`` (also the Kafka message key)

The replay is identified as a simulation by ``event_type`` (``saber11.result.replayed``).
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from src.streaming import settings

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "saber11_result_event.v1.schema.json"

# Literal types are built from the shared domains so the contract can never drift from DQ-09/DQ-10.
Naturaleza = Literal["OFICIAL", "NO OFICIAL", "No aplica"]
Jornada = Literal["COMPLETA", "UNICA", "MAÑANA", "TARDE", "NOCHE", "SABATINA", "No aplica"]
Area = Literal["URBANO", "RURAL", "No aplica"]
Genero = Literal["F", "M", "No informado"]

_g_lo, _g_hi = settings.RANGE_GLOBAL
_a_lo, _a_hi = settings.RANGE_AREA


class Saber11ResultEvent(BaseModel):
    """Contract v1. All scores are REQUIRED (decision of the team: the source always reports them)."""

    model_config = ConfigDict(extra="forbid", strict=True)

    # --- envelope
    event_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$",
                          description="UUIDv5 of 'estu_consecutivo|periodo'")
    event_type: Literal["saber11.result.replayed"]
    event_version: Literal[1]
    event_ts: datetime = Field(description="UTC timestamp of publication (ISO-8601)")

    # --- key and business context
    cod_dane_depto: str = Field(pattern=r"^\d{2}$", description="Event key: 2-digit DANE code, '00' = validante")
    estu_consecutivo: str = Field(min_length=1, max_length=30)
    periodo: str = Field(pattern=r"^\d{4}-[12]$")
    cole_naturaleza: Naturaleza
    cole_jornada: Jornada
    cole_area_ubicacion: Area
    estu_genero: Genero

    # --- measures (all required)
    punt_lectura_critica: float = Field(ge=_a_lo, le=_a_hi)
    punt_matematicas: float = Field(ge=_a_lo, le=_a_hi)
    punt_sociales_ciudadanas: float = Field(ge=_a_lo, le=_a_hi)
    punt_c_naturales: float = Field(ge=_a_lo, le=_a_hi)
    punt_ingles: float = Field(ge=_a_lo, le=_a_hi)
    punt_global: float = Field(ge=_g_lo, le=_g_hi)
    puntaje_sospechoso: bool

    @model_validator(mode="after")
    def semantic_rules(self) -> "Saber11ResultEvent":
        # DQ-21: the department code must be one of the 33 DANE codes (or '00' for validantes)
        if self.cod_dane_depto not in settings.VALID_DEPTO_CODES:
            raise ValueError(f"DQ-21: cod_dane_depto '{self.cod_dane_depto}' is not a DANE department")
        # DQ-07: the suspicious flag is exactly 'punt_global == 0'
        if self.puntaje_sospechoso != (self.punt_global == 0):
            raise ValueError("DQ-07: puntaje_sospechoso must be true if and only if punt_global == 0")
        # DQ-11: a validante (no school) lives in department '00' and nowhere else
        no_school = self.cole_naturaleza == settings.NO_APLICA
        if no_school != (self.cod_dane_depto == settings.VALIDANTE_DEPTO):
            raise ValueError("DQ-11: 'No aplica' school <=> department '00'")
        if self.event_ts.tzinfo is None:
            raise ValueError("event_ts must carry a timezone (UTC)")
        return self

    @property
    def anio(self) -> int:
        """Calendar year of the semester: the conformed key shared with the MEN indicators (T-05)."""
        return int(self.periodo[:4])


def event_id_for(estu_consecutivo: str, periodo: str) -> str:
    """Deterministic identifier: replaying the same exam attempt always yields the same event_id."""
    return str(uuid.uuid5(uuid.UUID(settings.EVENT_ID_NAMESPACE), f"{estu_consecutivo}|{periodo}"))


def parse_event(raw: bytes | str) -> Saber11ResultEvent:
    """Validates a raw Kafka value. Raises ``ValidationError`` (schema/semantic) or ``ValueError`` (bad JSON)."""
    return Saber11ResultEvent.model_validate_json(raw, strict=True)


def describe_error(exc: Exception) -> str:
    """Compact, single-line reason that is stored in the invalid-events table and in the DLQ."""
    if isinstance(exc, ValidationError):
        parts = []
        for err in exc.errors(include_url=False)[:5]:
            loc = ".".join(str(p) for p in err["loc"]) or "(event)"
            parts.append(f"{loc}: {err['msg']}")
        return " | ".join(parts)
    return f"{type(exc).__name__}: {exc}"


def export_schema(path: Path = SCHEMA_PATH) -> Path:
    schema = Saber11ResultEvent.model_json_schema()
    schema["$id"] = "https://github.com/budget-to-brilliance/contracts/saber11_result_event.v1"
    schema["title"] = "Saber11ResultEvent v1"
    schema["x-event-key"] = "cod_dane_depto"
    schema["x-semantic-rules"] = [
        "DQ-21: cod_dane_depto in the 33 DANE codes or '00'",
        "DQ-07: puntaje_sospechoso == (punt_global == 0)",
        "DQ-11: cole_naturaleza == 'No aplica' <=> cod_dane_depto == '00'",
        "event_ts must include a timezone",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Event contract utilities")
    ap.add_argument("--export", action="store_true", help="write contracts/saber11_result_event.v1.schema.json")
    args = ap.parse_args()
    if args.export:
        print(f"schema written to {export_schema()}")
    else:
        print(json.dumps(Saber11ResultEvent.model_json_schema(), indent=2, ensure_ascii=False))
