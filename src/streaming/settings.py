"""Streaming configuration. Domains and DANE codes are imported from ``src.config`` (single source
of truth shared with the batch pipeline); only streaming-specific knobs are defined here."""
import os

from src import config

# ---------------------------------------------------------------- Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RESULTS = os.getenv("KAFKA_TOPIC_RESULTS", "saber11.resultados.v1")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "saber11.resultados.dlq.v1")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "btb-stream-metrics")

# ------------------------------------------------------------- consumer
CONSUMER_MAX_BATCH = int(os.getenv("STREAM_MAX_BATCH", "200"))          # events per micro-batch
CONSUMER_FLUSH_SECONDS = float(os.getenv("STREAM_FLUSH_SECONDS", "1"))   # max wait for a micro-batch
BUCKET_SECONDS = int(os.getenv("STREAM_BUCKET_SECONDS", "10"))           # width of the time series bucket
REFERENCE_REFRESH_SECONDS = int(os.getenv("STREAM_REFERENCE_REFRESH_SECONDS", "300"))

# ------------------------------------------------------------- contract
EVENT_TYPE = "saber11.result.replayed"
EVENT_VERSION = 1
# Namespace of the deterministic UUIDv5 event_id: same exam attempt -> same event_id, always.
EVENT_ID_NAMESPACE = "6f1c2d0e-0000-4000-8000-00000000b2b0"

NO_APLICA = config.NO_APLICA
NO_INFORMADO = "No informado"
VALIDANTE_DEPTO = "00"  # cod_dane_depto of the "-1 / not applicable" member of dim_departamento

DOMAIN_NATURALEZA = config.DOMAIN_NATURALEZA + [NO_APLICA]
DOMAIN_JORNADA = config.DOMAIN_JORNADA + [NO_APLICA]
DOMAIN_AREA = config.DOMAIN_AREA + [NO_APLICA]
DOMAIN_GENERO = ["F", "M", NO_INFORMADO]
VALID_DEPTO_CODES = set(config.DANE_DEPARTAMENTOS) | {VALIDANTE_DEPTO}

# Subject scores (0-100) and the global score (0-500): same ranges as DQ-05.
RANGE_GLOBAL = config.RANGE_PUNT_GLOBAL
RANGE_AREA = config.RANGE_PUNT_AREA
SUBJECT_FIELDS = {
    "lectura_critica": "punt_lectura_critica",
    "matematicas": "punt_matematicas",
    "sociales_ciudadanas": "punt_sociales_ciudadanas",
    "c_naturales": "punt_c_naturales",
    "ingles": "punt_ingles",
}
# "global" is the sixth measure of the gender gap (M4)
GENDER_MEASURES = {"global": "punt_global", **SUBJECT_FIELDS}

# Years with MEN indicators in the DW (project window of the batch pipeline).
MEN_FIRST_YEAR = config.MEN_MIN_YEAR_EXCLUSIVE + 1
MEN_LAST_YEAR = config.MEN_LAST_YEAR_EXPECTED

# ---------------------------------------------------------------- DB users
GRAFANA_DB_USER = os.getenv("GRAFANA_DB_USER", "grafana_ro")
GRAFANA_DB_PASSWORD = os.getenv("GRAFANA_DB_PASSWORD", "grafana_ro_local")
