"""Generates grafana/dashboards/streaming.json (so the dashboard is code, reviewable in Git).

    python scripts/build_grafana_dashboard.py

Every panel reads ONLY views/tables of the streaming store (v_stream_* / stream_*), through the
read-only datasource ``btb-mysql``. Dashboard variables: $anio (year to show), $bucket_seconds
(constant = STREAM_BUCKET_SECONDS of the consumer, default 10).
"""
import json
from pathlib import Path

DS = {"type": "mysql", "uid": "btb-mysql"}
OUT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards" / "streaming.json"
_next_id = iter(range(1, 100))


def target(sql: str, fmt: str = "table") -> list[dict]:
    return [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": sql.strip()}]


def panel(ptype, title, x, y, w, h, sql, fmt="table", description="", options=None, field_config=None):
    return {"id": next(_next_id), "type": ptype, "title": title, "description": description, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": target(sql, fmt),
            "options": options or {}, "fieldConfig": field_config or {"defaults": {}, "overrides": []}}


def stat(title, x, sql, unit="short", thresholds=None, description=""):
    steps = thresholds or [{"color": "blue", "value": None}]
    return panel("stat", title, x, 4, 4, 4, sql, description=description,
                 options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                          "colorMode": "value", "graphMode": "none", "textMode": "value"},
                 field_config={"defaults": {"unit": unit, "decimals": 1 if unit in ("ms", "percent") else 0,
                                            "thresholds": {"mode": "absolute", "steps": steps},
                                            "color": {"mode": "thresholds"}}, "overrides": []})


def timeseries(title, x, y, w, h, sql, unit="short", description=""):
    return panel("timeseries", title, x, y, w, h, sql, fmt="time_series", description=description,
                 options={"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                          "tooltip": {"mode": "multi", "sort": "none"}},
                 field_config={"defaults": {"unit": unit, "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 8,
                                                                      "showPoints": "auto", "spanNulls": True}}, "overrides": []})


def bars(title, x, y, w, h, sql, xfield, horizontal=True, unit="short", description=""):
    return panel("barchart", title, x, y, w, h, sql, description=description,
                 options={"orientation": "horizontal" if horizontal else "vertical", "xField": xfield,
                          "showValue": "auto", "barWidth": 0.8, "legend": {"showLegend": True, "placement": "bottom"},
                          "tooltip": {"mode": "single"}, "xTickLabelRotation": 0, "stacking": "none"},
                 field_config={"defaults": {"unit": unit, "color": {"mode": "palette-classic"}}, "overrides": []})


def scatter(title, x, y, sql, xcol, description=""):
    return panel("xychart", title, x, y, 12, 9, sql, description=description,
                 options={"mapping": "auto", "seriesMapping": "auto", "dims": {"x": xcol},
                          "legend": {"showLegend": False, "placement": "bottom"}, "tooltip": {"mode": "single"}},
                 field_config={"defaults": {"custom": {"show": "points", "pointSize": {"fixed": 9}}}, "overrides": []})


intro = {
    "id": next(_next_id), "type": "text", "title": "", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 4},
    "options": {"mode": "markdown", "content": (
        "### Saber 11 — monitor en tiempo real *(replay simulado)*\n"
        "Los eventos son **registros históricos** de `fact_resultado_saber11` que el productor reproduce en Kafka "
        "(`saber11.result.replayed`). El consumidor valida cada evento con el contrato v1 y actualiza estas métricas "
        "en MySQL cada ~1 s; el panel se refresca cada 5 s.  \n"
        "**Diferencia con el dashboard histórico (Power BI):** aquel lee el Data Warehouse completo después de cada "
        "full refresh del DAG; este lee acumuladores `stream_*` que cambian evento a evento y muestra la salud del flujo. "
        "Los valores son **parciales** (muestra del replay): son asociaciones, no causalidad.")},
}

panels = [intro]
panels += [
    stat("Eventos válidos", 0, "SELECT n_validos FROM v_stream_estado",
         description="Eventos aplicados a las métricas (sin duplicados ni inválidos)."),
    stat("Eventos inválidos (DLQ)", 4, "SELECT n_invalidos FROM v_stream_estado",
         thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
         description="Rechazados por el contrato v1; quedan en stream_eventos_invalidos y en el topic DLQ."),
    stat("Duplicados ignorados", 8, "SELECT n_duplicados FROM v_stream_estado",
         thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}],
         description="Mismo event_id ya aplicado: no se suma dos veces."),
    stat("Sin datos hace (s)", 12, "SELECT segundos_sin_datos FROM v_stream_estado", unit="s",
         thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 15}, {"color": "red", "value": 60}],
         description="Frescura: segundos desde el último micro-lote aplicado. Sube si el consumidor se detiene."),
    stat("% inválidos", 16, "SELECT pct_invalidos FROM v_stream_estado", unit="percent",
         thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}, {"color": "red", "value": 5}]),
    stat("Latencia media, último minuto (ms)", 20,
         "SELECT SUM(suma_latencia_ms) / NULLIF(SUM(n_latencia), 0) AS latencia_ms FROM stream_serie_tiempo "
         "WHERE bucket_ts >= UTC_TIMESTAMP() - INTERVAL 60 SECOND", unit="ms",
         description="Desde event_ts (publicación) hasta que el consumidor aplica el evento."),
]
panels += [
    timeseries("Eventos por segundo (M5)", 0, 8, 12, 8,
               "SELECT bucket_ts AS time, n_validos / $bucket_seconds AS `válidos/s`, n_invalidos / $bucket_seconds AS `inválidos/s`, "
               "n_duplicados / $bucket_seconds AS `duplicados/s` FROM stream_serie_tiempo WHERE $__timeFilter(bucket_ts) ORDER BY bucket_ts",
               unit="ops", description="Throughput del consumidor por bucket de STREAM_BUCKET_SECONDS."),
    timeseries("Brechas acumuladas en el tiempo (M2, M3, M4)", 12, 8, 12, 8,
               "SELECT bucket_ts AS time, brecha_no_oficial_oficial_acum AS `No oficial − oficial (R1)`, "
               "brecha_urbano_rural_acum AS `Urbano − rural (R3)`, brecha_genero_global_acum AS `Hombres − mujeres, global (R5)` "
               "FROM stream_serie_tiempo WHERE $__timeFilter(bucket_ts) ORDER BY bucket_ts",
               unit="short", description="Brechas nacionales acumuladas (puntos del puntaje global): convergen a medida que llegan eventos."),
    bars("Puntaje global medio por departamento (M1)", 0, 16, 12, 12,
         "SELECT nombre_departamento AS departamento, prom_global AS `Puntaje global medio` FROM v_stream_departamento "
         "WHERE anio = $anio AND n_score > 0 ORDER BY prom_global DESC",
         "departamento", description="Excluye validantes (depto 00) y puntajes sospechosos, como R4/R6."),
    bars("Brecha de género por asignatura, M − F (M4 · R5)", 12, 16, 12, 6,
         "SELECT medida AS asignatura, brecha_m_menos_f AS `Hombres − mujeres` FROM v_stream_genero_nacional "
         "WHERE anio = $anio ORDER BY medida", "asignatura", horizontal=False,
         description="Puntaje global y las 5 asignaturas (lectura crítica, matemáticas, sociales y ciudadanas, ciencias naturales, inglés)."),
    panel("table", "Brechas por departamento (M2 · M3)", 12, 22, 12, 6,
          "SELECT nombre_departamento AS Departamento, n_score AS `N con puntaje`, brecha_no_oficial_oficial AS `No oficial − oficial`, "
          "brecha_urbano_rural AS `Urbano − rural` FROM v_stream_departamento WHERE anio = $anio AND n_score > 0 "
          "ORDER BY n_score DESC",
          description="Con pocos eventos por departamento las brechas son ruidosas: mire N."),
    scatter("Puntaje vs deserción por departamento (M6 · R6)", 0, 28,
            "SELECT nombre_departamento AS departamento, desercion_media, prom_global AS `Puntaje global medio` "
            "FROM v_stream_riesgo WHERE anio = $anio", "desercion_media",
            description="Deserción: API del MEN (snapshot de fact_indicador_departamento). Puntaje: acumulado en streaming."),
    scatter("Puntaje vs repitencia por departamento (M7 · R7)", 12, 28,
            "SELECT nombre_departamento AS departamento, repitencia_secundaria, prom_global AS `Puntaje global medio` "
            "FROM v_stream_riesgo WHERE anio = $anio", "repitencia_secundaria",
            description="Repitencia en secundaria: API del MEN. Puntaje: acumulado en streaming."),
    panel("table", "Cuadrantes de riesgo (M6 · M7)", 0, 37, 16, 8,
          "SELECT nombre_departamento AS Departamento, n_score AS N, prom_global AS `Puntaje medio`, desercion_media AS `Deserción %`, "
          "cuadrante_desercion AS `Cuadrante deserción`, repitencia_secundaria AS `Repitencia %`, cuadrante_repitencia AS `Cuadrante repitencia` "
          "FROM v_stream_riesgo WHERE anio = $anio ORDER BY desercion_media DESC",
          description="Umbral = promedio de los departamentos del mismo año con datos hasta el momento."),
    panel("table", "Spearman por año (M6 · M7)", 16, 37, 8, 4,
          "SELECT anio AS Año, n_departamentos AS Deptos, spearman_desercion_vs_puntaje AS `ρ deserción`, "
          "spearman_repitencia_vs_puntaje AS `ρ repitencia` FROM v_stream_spearman ORDER BY anio",
          description="Misma aproximación del batch (RANK sin corrección de empates)."),
    panel("table", "Últimos eventos rechazados (contrato v1)", 16, 41, 8, 4,
          "SELECT recibido_en AS `Recibido (UTC)`, kafka_partition AS `Partición`, kafka_offset AS `Offset`, motivo AS `Motivo` "
          "FROM stream_eventos_invalidos ORDER BY recibido_en DESC LIMIT 10",
          description="Auditoría de la validación: cada rechazo guarda el motivo y su posición en Kafka."),
]

dashboard = {
    "uid": "btb-stream", "title": "Saber 11 — Streaming en tiempo real (replay simulado)", "tags": ["btb", "streaming", "kafka"],
    "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": False, "graphTooltip": 1,
    "refresh": "5s", "time": {"from": "now-15m", "to": "now"}, "timepicker": {"refresh_intervals": ["1s", "5s", "10s", "30s", "1m"]},
    "description": "Métricas de streaming calculadas por el consumidor Kafka (stream_* en MySQL).",
    "templating": {"list": [
        {"name": "anio", "label": "Año", "type": "query", "datasource": DS, "refresh": 2, "sort": 2, "includeAll": False, "multi": False,
         "query": "SELECT DISTINCT anio FROM stream_metricas_departamento ORDER BY anio DESC",
         "definition": "SELECT DISTINCT anio FROM stream_metricas_departamento ORDER BY anio DESC"},
        {"name": "bucket_seconds", "label": "Bucket (s)", "type": "constant", "query": "10", "hide": 2,
         "description": "Debe coincidir con STREAM_BUCKET_SECONDS del consumidor."},
    ]},
    "annotations": {"list": []}, "panels": panels,
}

if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"{len(panels)} panels -> {OUT}")
