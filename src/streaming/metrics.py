"""Pure metric logic (no Kafka, no database): turns a micro-batch of VALID events into increments.

Eligibility rules (inherited from the batch pipeline, so streaming and batch agree):
  * puntaje_sospechoso (punt_global == 0) -> counted as event, NEVER enters any score (DQ-07, T-08)
  * M2 official gap   -> only OFICIAL / NO OFICIAL schools ('No aplica' = validantes are left out, like R1)
  * M3 urban-rural    -> only URBANO / RURAL (like R3)
  * M4 gender gap     -> only F / M ('No informado' is left out, like R5); global + the 5 subjects
  * validantes live in department '00'; the views exclude '00' from the departmental metrics (R4, R6, R7)
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from src.streaming import settings
from src.streaming.contract import Saber11ResultEvent

# Order matters: it is the column order used by the SQL upsert in store.py
DEPT_COUNTERS = [
    "n_eventos", "n_sospechosos", "n_score", "suma_global",
    "n_oficial", "suma_oficial", "n_no_oficial", "suma_no_oficial",
    "n_urbano", "suma_urbano", "n_rural", "suma_rural",
]


@dataclass
class BatchAggregate:
    """Increments produced by one micro-batch."""

    dept: dict[tuple[str, int], dict[str, float]] = field(
        default_factory=lambda: defaultdict(lambda: {k: 0.0 for k in DEPT_COUNTERS}))
    gender: dict[tuple[str, int, str, str], list[float]] = field(
        default_factory=lambda: defaultdict(lambda: [0.0, 0.0]))  # (cod, anio, medida, genero) -> [n, suma]
    n_events: int = 0
    n_suspicious: int = 0
    n_sin_referencia: int = 0
    sum_latency_ms: float = 0.0
    n_latency: int = 0
    last_event_ts: datetime | None = None


def naive_utc(ts: datetime) -> datetime:
    """MySQL DATETIME has no time zone: the project convention is 'always UTC, stored naive'."""
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


def aggregate(events: Iterable[Saber11ResultEvent], reference_keys: set[tuple[str, int]],
              now: datetime) -> BatchAggregate:
    """``now`` is the processing time as a naive UTC datetime (used for the latency measure)."""
    agg = BatchAggregate()
    for ev in events:
        agg.n_events += 1
        ts = naive_utc(ev.event_ts)
        if agg.last_event_ts is None or ts > agg.last_event_ts:
            agg.last_event_ts = ts
        agg.sum_latency_ms += max((now - ts).total_seconds() * 1000.0, 0.0)
        agg.n_latency += 1

        # Events without MEN context (e.g. periods outside 2022-2024) are counted, not hidden.
        # Validantes (department '00') are not expected to have API data.
        if ev.cod_dane_depto != settings.VALIDANTE_DEPTO and (ev.cod_dane_depto, ev.anio) not in reference_keys:
            agg.n_sin_referencia += 1

        acc = agg.dept[(ev.cod_dane_depto, ev.anio)]
        acc["n_eventos"] += 1
        if ev.puntaje_sospechoso:
            acc["n_sospechosos"] += 1
            agg.n_suspicious += 1
            continue

        acc["n_score"] += 1
        acc["suma_global"] += ev.punt_global
        if ev.cole_naturaleza == "OFICIAL":
            acc["n_oficial"] += 1
            acc["suma_oficial"] += ev.punt_global
        elif ev.cole_naturaleza == "NO OFICIAL":
            acc["n_no_oficial"] += 1
            acc["suma_no_oficial"] += ev.punt_global
        if ev.cole_area_ubicacion == "URBANO":
            acc["n_urbano"] += 1
            acc["suma_urbano"] += ev.punt_global
        elif ev.cole_area_ubicacion == "RURAL":
            acc["n_rural"] += 1
            acc["suma_rural"] += ev.punt_global

        if ev.estu_genero in ("F", "M"):
            for medida, attr in settings.GENDER_MEASURES.items():
                cell = agg.gender[(ev.cod_dane_depto, ev.anio, medida, ev.estu_genero)]
                cell[0] += 1
                cell[1] += getattr(ev, attr)
    return agg


def bucket_of(now: datetime, seconds: int = settings.BUCKET_SECONDS) -> datetime:
    """Start of the time-series bucket (naive UTC) that contains ``now``."""
    epoch = int(now.replace(tzinfo=timezone.utc).timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, tz=timezone.utc).replace(tzinfo=None)
