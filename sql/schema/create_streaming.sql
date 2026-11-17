-- ============================================================================
-- Budget To Brilliance - Final Delivery: STREAMING store [MySQL 8]
-- Same database as the Data Warehouse (icfes_dw), separate objects with the prefix stream_ / v_stream_.
--
-- Design rules
--  * Idempotent: CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW, nothing is ever dropped.
--  * NO foreign keys towards the DW dimensions and NOT listed in load_all.empty_warehouse(): the
--    batch full refresh (TRUNCATE of the DW) can never block on, or wipe, the streaming tables.
--  * The consumer stores SUMS and COUNTS (not averages): the incremental mean stays exact and the
--    gaps are computed in the views.
--  * Timestamps are UTC (the consumer writes them; it does not rely on the server time zone).
--
-- Metrics (see README, section 8):
--   M1 mean global score per department and year ........ stream_metricas_departamento
--   M2 gap non-official - official (R1) ................... stream_metricas_departamento
--   M3 gap urban - rural (R3) ............................. stream_metricas_departamento
--   M4 gender gap (M - F), global AND the 5 subjects (R5) . stream_metricas_genero
--   M5 stream quality: invalid, duplicates, throughput,
--      latency, freshness ................................. stream_estado + stream_serie_tiempo
--   M6/M7 score vs MEN dropout / repetition (R6, R7) ...... v_stream_riesgo (uses the MEN snapshot)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Idempotency ledger: one row per event already applied to the metrics
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_eventos_procesados (
    event_id      CHAR(36)    NOT NULL PRIMARY KEY,
    processed_at  DATETIME(3) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- Events rejected by the contract (also published to the DLQ topic).
-- PK = Kafka position, so reprocessing the same message can never log it twice.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_eventos_invalidos (
    kafka_partition  INT          NOT NULL,
    kafka_offset     BIGINT       NOT NULL,
    kafka_key        VARCHAR(64),
    motivo           VARCHAR(600) NOT NULL,
    payload          TEXT,
    recibido_en      DATETIME(3)  NOT NULL,
    PRIMARY KEY (kafka_partition, kafka_offset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- M1, M2, M3 accumulators. Grain: department x calendar year (the integration grain of the
-- Second Delivery, so the MEN indicators join without fan-out).
-- Only NON-suspicious events feed the score columns (DQ-07).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_metricas_departamento (
    cod_dane_depto   CHAR(2)       NOT NULL,
    anio             INT           NOT NULL,
    n_eventos        BIGINT        NOT NULL DEFAULT 0,
    n_sospechosos    BIGINT        NOT NULL DEFAULT 0,
    n_score          BIGINT        NOT NULL DEFAULT 0,
    suma_global      DECIMAL(18,1) NOT NULL DEFAULT 0,
    n_oficial        BIGINT        NOT NULL DEFAULT 0,
    suma_oficial     DECIMAL(18,1) NOT NULL DEFAULT 0,
    n_no_oficial     BIGINT        NOT NULL DEFAULT 0,
    suma_no_oficial  DECIMAL(18,1) NOT NULL DEFAULT 0,
    n_urbano         BIGINT        NOT NULL DEFAULT 0,
    suma_urbano      DECIMAL(18,1) NOT NULL DEFAULT 0,
    n_rural          BIGINT        NOT NULL DEFAULT 0,
    suma_rural       DECIMAL(18,1) NOT NULL DEFAULT 0,
    updated_at       DATETIME(3)   NOT NULL,
    PRIMARY KEY (cod_dane_depto, anio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- M4 accumulators (R5): one row per department x year x measure x gender.
-- measure = global | lectura_critica | matematicas | sociales_ciudadanas | c_naturales | ingles
-- Long format on purpose: 6 measures x 2 genders would be 24 columns in a wide table.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_metricas_genero (
    cod_dane_depto  CHAR(2)       NOT NULL,
    anio            INT           NOT NULL,
    medida          VARCHAR(30)   NOT NULL,
    estu_genero     CHAR(1)       NOT NULL,
    n               BIGINT        NOT NULL DEFAULT 0,
    suma            DECIMAL(18,1) NOT NULL DEFAULT 0,
    PRIMARY KEY (cod_dane_depto, anio, medida, estu_genero)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- M5 (global state, a single row) and the time series used by the dashboard
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_estado (
    id                TINYINT     NOT NULL PRIMARY KEY,
    n_validos         BIGINT      NOT NULL DEFAULT 0,
    n_invalidos       BIGINT      NOT NULL DEFAULT 0,
    n_duplicados      BIGINT      NOT NULL DEFAULT 0,
    n_sin_referencia  BIGINT      NOT NULL DEFAULT 0,
    last_event_ts     DATETIME(3) NULL,
    last_processed_at DATETIME(3) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO stream_estado (id) VALUES (1);

CREATE TABLE IF NOT EXISTS stream_serie_tiempo (
    bucket_ts                       DATETIME NOT NULL PRIMARY KEY,
    n_validos                       BIGINT   NOT NULL DEFAULT 0,
    n_invalidos                     BIGINT   NOT NULL DEFAULT 0,
    n_duplicados                    BIGINT   NOT NULL DEFAULT 0,
    n_sospechosos                   BIGINT   NOT NULL DEFAULT 0,
    suma_latencia_ms                DOUBLE   NOT NULL DEFAULT 0,
    n_latencia                      BIGINT   NOT NULL DEFAULT 0,
    prom_global_acum                DECIMAL(7,2) NULL,
    brecha_no_oficial_oficial_acum  DECIMAL(7,2) NULL,
    brecha_urbano_rural_acum        DECIMAL(7,2) NULL,
    brecha_genero_global_acum       DECIMAL(7,2) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- MEN (API) reference snapshot. The consumer copies it from the DW (fact_indicador_departamento,
-- loaded by the Airflow API branch) and keeps the copy, so the dashboard keeps working while the
-- batch full refresh empties the DW.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_referencia_departamento (
    cod_dane_depto       CHAR(2)      NOT NULL PRIMARY KEY,
    nombre_departamento  VARCHAR(80)  NOT NULL,
    loaded_at            DATETIME(3)  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stream_referencia_indicador (
    cod_dane_depto         CHAR(2)       NOT NULL,
    anio                   INT           NOT NULL,
    desercion_media        DECIMAL(5,2)  NOT NULL,
    repitencia_secundaria  DECIMAL(5,2)  NOT NULL,
    loaded_at              DATETIME(3)   NOT NULL,
    PRIMARY KEY (cod_dane_depto, anio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- VIEWS (what Grafana reads). Gap conventions follow the First/Second Delivery:
--   official gap  = mean(NO OFICIAL) - mean(OFICIAL)      (R1)
--   urban-rural   = mean(URBANO) - mean(RURAL)            (R3)
--   gender gap    = mean(M) - mean(F)                     (R5)
-- ============================================================================

-- M1, M2, M3 by department and year (validantes, department '00', are excluded like in R4/R6/R7)
CREATE OR REPLACE VIEW v_stream_departamento AS
SELECT m.cod_dane_depto,
       COALESCE(r.nombre_departamento, m.cod_dane_depto) AS nombre_departamento,
       m.anio,
       m.n_eventos,
       m.n_sospechosos,
       m.n_score,
       ROUND(m.suma_global / NULLIF(m.n_score, 0), 1)             AS prom_global,
       m.n_oficial,
       m.n_no_oficial,
       ROUND(m.suma_oficial / NULLIF(m.n_oficial, 0), 1)          AS prom_oficial,
       ROUND(m.suma_no_oficial / NULLIF(m.n_no_oficial, 0), 1)    AS prom_no_oficial,
       ROUND(m.suma_no_oficial / NULLIF(m.n_no_oficial, 0)
             - m.suma_oficial / NULLIF(m.n_oficial, 0), 1)        AS brecha_no_oficial_oficial,
       m.n_urbano,
       m.n_rural,
       ROUND(m.suma_urbano / NULLIF(m.n_urbano, 0), 1)            AS prom_urbano,
       ROUND(m.suma_rural / NULLIF(m.n_rural, 0), 1)              AS prom_rural,
       ROUND(m.suma_urbano / NULLIF(m.n_urbano, 0)
             - m.suma_rural / NULLIF(m.n_rural, 0), 1)            AS brecha_urbano_rural,
       m.updated_at
FROM stream_metricas_departamento m
LEFT JOIN stream_referencia_departamento r ON r.cod_dane_depto = m.cod_dane_depto
WHERE m.cod_dane_depto <> '00';

-- M4 by department (global + 5 subjects)
CREATE OR REPLACE VIEW v_stream_genero_depto AS
SELECT g.cod_dane_depto,
       COALESCE(r.nombre_departamento, g.cod_dane_depto) AS nombre_departamento,
       g.anio,
       g.medida,
       SUM(CASE WHEN g.estu_genero = 'M' THEN g.n ELSE 0 END)    AS n_m,
       SUM(CASE WHEN g.estu_genero = 'F' THEN g.n ELSE 0 END)    AS n_f,
       ROUND(SUM(CASE WHEN g.estu_genero = 'M' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'M' THEN g.n END), 0), 1) AS prom_m,
       ROUND(SUM(CASE WHEN g.estu_genero = 'F' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'F' THEN g.n END), 0), 1) AS prom_f,
       ROUND(SUM(CASE WHEN g.estu_genero = 'M' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'M' THEN g.n END), 0)
           - SUM(CASE WHEN g.estu_genero = 'F' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'F' THEN g.n END), 0), 2)     AS brecha_m_menos_f
FROM stream_metricas_genero g
LEFT JOIN stream_referencia_departamento r ON r.cod_dane_depto = g.cod_dane_depto
WHERE g.cod_dane_depto <> '00'
GROUP BY g.cod_dane_depto, COALESCE(r.nombre_departamento, g.cod_dane_depto), g.anio, g.medida;

-- M4 national (all examinees with a reported gender, validantes included, like R5)
CREATE OR REPLACE VIEW v_stream_genero_nacional AS
SELECT g.anio,
       g.medida,
       SUM(CASE WHEN g.estu_genero = 'M' THEN g.n ELSE 0 END)    AS n_m,
       SUM(CASE WHEN g.estu_genero = 'F' THEN g.n ELSE 0 END)    AS n_f,
       ROUND(SUM(CASE WHEN g.estu_genero = 'M' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'M' THEN g.n END), 0), 1) AS prom_m,
       ROUND(SUM(CASE WHEN g.estu_genero = 'F' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'F' THEN g.n END), 0), 1) AS prom_f,
       ROUND(SUM(CASE WHEN g.estu_genero = 'M' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'M' THEN g.n END), 0)
           - SUM(CASE WHEN g.estu_genero = 'F' THEN g.suma END)
             / NULLIF(SUM(CASE WHEN g.estu_genero = 'F' THEN g.n END), 0), 2)     AS brecha_m_menos_f
FROM stream_metricas_genero g
GROUP BY g.anio, g.medida;

-- M6/M7 base: streaming score joined with the MEN indicators at the integration grain (department x year)
CREATE OR REPLACE VIEW v_stream_riesgo_base AS
SELECT d.cod_dane_depto, d.nombre_departamento, d.anio, d.n_score, d.prom_global,
       i.desercion_media, i.repitencia_secundaria
FROM v_stream_departamento d
JOIN stream_referencia_indicador i
  ON i.cod_dane_depto = d.cod_dane_depto AND i.anio = d.anio
WHERE d.n_score > 0;

-- M6/M7: bivariate quadrants. Thresholds = mean across the departments of the same year
-- (same logic as the bivariate risk quadrants of the batch dashboard, but on the live partial data).
CREATE OR REPLACE VIEW v_stream_riesgo AS
SELECT b.cod_dane_depto, b.nombre_departamento, b.anio, b.n_score, b.prom_global,
       b.desercion_media, b.repitencia_secundaria,
       CASE WHEN b.desercion_media > AVG(b.desercion_media) OVER (PARTITION BY b.anio)
                 AND b.prom_global < AVG(b.prom_global) OVER (PARTITION BY b.anio) THEN 'Alta desercion / bajo puntaje'
            WHEN b.desercion_media > AVG(b.desercion_media) OVER (PARTITION BY b.anio) THEN 'Alta desercion / alto puntaje'
            WHEN b.prom_global < AVG(b.prom_global) OVER (PARTITION BY b.anio)         THEN 'Baja desercion / bajo puntaje'
            ELSE 'Baja desercion / alto puntaje' END AS cuadrante_desercion,
       CASE WHEN b.repitencia_secundaria > AVG(b.repitencia_secundaria) OVER (PARTITION BY b.anio)
                 AND b.prom_global < AVG(b.prom_global) OVER (PARTITION BY b.anio) THEN 'Alta repitencia / bajo puntaje'
            WHEN b.repitencia_secundaria > AVG(b.repitencia_secundaria) OVER (PARTITION BY b.anio) THEN 'Alta repitencia / alto puntaje'
            WHEN b.prom_global < AVG(b.prom_global) OVER (PARTITION BY b.anio)                     THEN 'Baja repitencia / bajo puntaje'
            ELSE 'Baja repitencia / alto puntaje' END AS cuadrante_repitencia
FROM v_stream_riesgo_base b;

-- M6/M7 summary: Spearman rank correlation per year (same approximation as the batch SQL: RANK, no tie correction)
CREATE OR REPLACE VIEW v_stream_spearman AS
WITH ranked AS (
    SELECT anio,
           RANK() OVER (PARTITION BY anio ORDER BY prom_global)           AS r_puntaje,
           RANK() OVER (PARTITION BY anio ORDER BY desercion_media)       AS r_desercion,
           RANK() OVER (PARTITION BY anio ORDER BY repitencia_secundaria) AS r_repitencia,
           COUNT(*)   OVER (PARTITION BY anio)                            AS n
    FROM v_stream_riesgo_base
)
SELECT anio,
       MAX(n) AS n_departamentos,
       ROUND(1 - 6 * SUM(POW(CAST(r_puntaje AS SIGNED) - CAST(r_desercion AS SIGNED), 2))
                 / NULLIF(MAX(n) * (POW(MAX(n), 2) - 1), 0), 3)  AS spearman_desercion_vs_puntaje,
       ROUND(1 - 6 * SUM(POW(CAST(r_puntaje AS SIGNED) - CAST(r_repitencia AS SIGNED), 2))
                 / NULLIF(MAX(n) * (POW(MAX(n), 2) - 1), 0), 3)  AS spearman_repitencia_vs_puntaje
FROM ranked
GROUP BY anio;

-- M5: global state and freshness
CREATE OR REPLACE VIEW v_stream_estado AS
SELECT e.n_validos, e.n_invalidos, e.n_duplicados, e.n_sin_referencia,
       e.last_event_ts, e.last_processed_at,
       TIMESTAMPDIFF(SECOND, e.last_processed_at, UTC_TIMESTAMP()) AS segundos_sin_datos,
       ROUND(100 * e.n_invalidos / NULLIF(e.n_validos + e.n_invalidos, 0), 2) AS pct_invalidos
FROM stream_estado e
WHERE e.id = 1;
