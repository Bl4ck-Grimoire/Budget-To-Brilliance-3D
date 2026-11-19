# ETL Project. Data Engineering for Sustainable Development in Colombia, Final delivery

| Delivery | Repository | Status |
|---|---|---|
| First (1D) | https://github.com/Johann0811/Budget-To-Brilliance<br>https://github.com/Bl4ck-Grimoire/Budget-To-Brilliance | unchanged |
| Second (2D) | https://github.com/Johann0811/Budget-To-Brilliance-2D<br>https://github.com/Bl4ck-Grimoire/Budget-To-Brilliance-2D | unchanged |
| Final (this repo) | https://github.com/Johann0811/Budget-To-Brilliance-3D<br>https://github.com/Bl4ck-Grimoire/Budget-To-Brilliance-3D | New |


## Table of contents

1. [Project Overview and Objectives](#1-project-overview-and-objectives)
2. [Data Sources and EDA](#2-data-sources-and-eda)
3. [System Architecture](#3-system-architecture)
4. [Batch ETL Pipeline](#4-batch-etl-pipeline)
5. [Data Quality](#5-data-quality)
6. [Data Warehouse](#6-data-warehouse)
7. [Batch Analytical Dashboard](#7-batch-analytical-dashboard)
8. [Streaming Pipeline](#8-streaming-pipeline)
9. [Real-Time Dashboard](#9-real-time-dashboard)
10. [Execution and Reproducibility](#10-execution-and-reproducibility)
11. [Functional Evidence](#11-functional-evidence)
12. [Engineering Decisions and Limitations](#12-engineering-decisions-and-limitations)
13. [Repository structure and team](#13-repository-structure-and-team)

---

## 1. Project Overview and Objectives

### 1.1 Problem
Standardized results (ICFES Saber 11) are usually read in isolation from the trajectory of the students who never reach the exam. Saber 11 only observes students who persisted to the end of secondary education (survivorship bias), while the Ministry of Education (MEN) publishes departmental dropout and repetition rates with no link to learning outcomes. The project integrates both sources to monitor gaps (school sector, urban/rural, department, gender) and the relationship between performance and school trajectory.

### 1.2 Objectives

General objective: Integrate ICFES Saber 11 with the MEN trajectory indicators in an automated, validated and reproducible data, system that answers the analytical requirements R1–R7 historically (batch) and keeps their key gaps, and the health of the data flow, observable within seconds (streaming).

The objectives are the system goals; the analytical questions they must answer are the requirements of 1.3.

| ID | Objective | Type | Requirements served | Where it is met |
|---|---|---|---|---|
| O1 | Extract the ICFES files and the MEN API automatically and reproducibly | Batch | R1–R7 | [Batch ETL Pipeline](#4-batch-etl-pipeline) |
| O2 | Guarantee quality with 3 Great Expectations gates; critical failures never reach the DW | Batch | R1–R7 | [Data Quality](#5-data-quality) |
| O3 | Load a dimensional model (fact constellation) in MySQL | Batch | R1–R7 | [Data Warehouse](#6-data-warehouse) |
| O4 | Answer R1–R7 historically from the validated DW | Batch | R1–R7 | [Batch Analytical Dashboard](#7-batch-analytical-dashboard) |
| O5 | Process result events with low latency, validating each one against an explicit contract | Streaming | SR3 | [Streaming Pipeline](#8-streaming-pipeline) |
| O6 | Keep live gap, quality and risk metrics updated as events arrive | Streaming | R1, R3, R4, R5, R6, R7 (via M1–M7) | [Streaming Pipeline](#8-streaming-pipeline) and [Real-Time Dashboard](#9-real-time-dashboard) |
| O7 | Show the behaviour of the stream under invalid events and consumer interruption | Streaming | SR3 | [Behaviour with an invalid event and with an interrupted consumer](#87-behaviour-with-an-invalid-event-and-with-an-interrupted-consumer) and [Functional Evidence](#11-functional-evidence) |

### 1.3 Analytical requirements 

These are the requirements defined in the First Delivery (R1–R5), maintained or refined in the Second Delivery, which added R6 and R7
full definitions and integration strategy: ([README_2D](docs/batch/README_2D.md#3-status-and-changes-for-r1r5) · Sections 3 and 4.)

| ID | Requirement | Changes | Which insight does the data deliver? | How the objective was achieved |
|---|---|---|---|---|
| **R1** | Official vs. non-official school gap in the global score (excludes examinees without a school) | None | Non-official-school students score higher on average than official-school students in every period; the gap narrowed from 31.1 pts (2021-2) to 26.8 pts (2025-2) (1D finding). A descriptive association, not causality. | **Batch:** Power BI page 1 and `analytical_queries.sql` over the validated DW. **Live:** M2 keeps the gap updated as events arrive. |
| **R2** | Score gap by school shift (*jornada*) within sector | None | Full-day (`COMPLETA`) and single-shift (`UNICA`, within official schools) lead; night and Saturday shifts lag in both sectors, roughly 40–90 pts below (1D finding). | **Batch:** Power BI page 1. **Live:** not monitored (batch only). |
| **R3** | Urban–rural gap by department (top 10 largest gaps) | None | The departments with the widest urban–rural gaps (e.g. San Andrés, La Guajira, Guainía, Amazonas) are largely the same ones that rank lowest overall (1D finding). | **Batch:** Power BI page 1. **Live:** M3, national and by department. |
| **R4** | Departmental performance ranking across periods | None | Boyacá, Santander, Bogotá, Cundinamarca, Quindío and Norte de Santander lead (~266–273 pts); Chocó, Vaupés, Vichada, Guainía and Amazonas lag (~210–221) (1D finding). | **Batch:** Power BI page 2. **Live:** M1, mean global score by department. |
| **R5** | Gender gap (M−F) in the global score and the subjects | None | Male students score higher than female students in every period (~7.03 pts on average) and in several subjects, although there are more female examinees (1D finding). | **Batch:** Power BI page 2 and `analytical_queries.sql` by subject area. **Live:** M4 in the global score and the 5 subjects. |
| **R6** | Upper-secondary dropout (MEN API) vs. Saber 11 score, by department and year | None | Whether departments with higher dropout also score lower: the link the exam alone cannot show, because it only observes students who persisted (survivorship bias). Association, not causality. | **Batch:** Power BI page 3 and SQL (per-year Spearman) on the integrated department × year panel (99 observations). **Live:** M6. |
| **R7** | Secondary repetition (MEN API) vs. Saber 11 score, by department and year | None | Whether higher secondary repetition goes with lower scores at department level. Association, not causality. | **Batch:** Power BI page 3. **Live:** M7. |

(1D finding) = documented in the First Delivery README (R1-R5) computed on the same ICFES data.
R6 and R7 are the requirements that integrate both sources: neither the ICFES file nor the MEN API can answer them alone.

### 1.4 Expected outcomes
* A Data Warehouse loaded only with data that passed the critical quality gates, reproducible from a clean checkout.
* Historical answers to R1–R7 (Power BI over the DW) and the evidence that bad data stops the pipeline.
* A streaming pipeline that validates every event against a contract, never loses or double-counts an event, and updates seven live metrics (M1–M7).
* A real-time dashboard that visibly changes when new events are published, distinct from the historical one.
* Documented behaviour and limits of the stream under an invalid event and under a stopped consumer.

### 1.5 Batch vs. streaming requirements

| | **Batch (historical)** | **Streaming (low latency)** |
|---|---|---|
| Question | How large were the gaps over 2021-2…2025-2? | How are the gaps and the quality of the flow evolving as results arrive? |
| Data | Full DW (3.29M exam attempts, 99 department-year indicators) | Records replayed from the DW fact table, one event per exam attempt |
| Freshness | only after each manual refresh of the DAG | Automatically (consumer ≤ 1 s + dashboard refresh 5 s) |
| Quality control | 28 rules in 3 gates over whole batches | Event contract per message + invalid-event queue |
| Tool | Power BI over MySQL | Grafana over `stream_*` tables |

| ID | Streaming requirement | Linked to |
|---|---|---|
| SR1 | Monitor progress of results received, by department and year | R4 |
| SR2 | See how the official / urban-rural / gender gaps converge while events arrive (global score and the 5 subjects for gender) | R1, R3, R5 |
| SR3 | Watch the health of the stream: invalid, duplicates, throughput, latency, freshness | quality (DQ-05/07/21) |
| SR4 | Place each department live against the MEN dropout and repetition rates | R6, R7 |

### 1.6 Why streaming adds value
Saber 11 is historical data: nothing here actually needs sub-second latency, and the replay is a simulation. The value is operational and is realistic for the moment ICFES publishes a results release: a spike of suspicious (zero) scores or malformed events is detected within seconds instead of after the next full refresh of ~3.3M rows, provisional gaps are visible while the release is still being ingested and/or a stalled ingestion is visible immediately (freshness indicator). The architecture also shows the
pattern that would be used if the source were truly continuous.

### 1.7 Are the objectives met?
| Objective / requirement | Evidence | Status |
|---|---|---|
| O1, O2, O3 (batch ETL, gates, DW) | Airflow runs, controlled critical failure, safe re-run ([11.1 Batch (Second Delivery runs, reused pipeline)](#111-batch-second-delivery-runs-reused-pipeline)) | Evidenced (2D runs, pipeline reused unchanged) |
| O4 · R1, R2, R3, R4, R5, R6 | Power BI pages ([7. Batch Analytical Dashboard](#7-batch-analytical-dashboard)) | Evidenced (batch dashboard) |
| O4 · R7 | `analytical_queries.sql` and live metric M7 | Evidenced (batch dashboard) |
| O5 (contract and low-latency processing) | Contract + 62 automated tests ([11.2 Streaming: automated verification (executed during construction)](#112-streaming-automated-verification-executed-during-construction)) | Verified by tests. |
| O6 · M1–M7 | Dashboard queries tested against the consumer's data ([11.2 Streaming: automated verification (executed during construction)](#112-streaming-automated-verification-executed-during-construction)) | Verified by tests. |
| O7 (invalid event, consumer stopped) | `scripts/demo_experiments.sh`, behaviour tested ([11.2 Streaming: automated verification (executed during construction)](#112-streaming-automated-verification-executed-during-construction)) | Verified by tests. |



---

## 2. Data Sources and EDA

### 2.1 Sources (Unchanged)

| Source | Type | Content | Volume |
|---|---|---|---|
| **ICFES Saber 11** (DataIcfes) | Static files `.txt` | Scores, school sector/shift/area/department, gender, 9 semesters 2021-2 … 2025-2 | 3,293,842 records, ~2.2 GB |
| **MEN API** (Socrata dataset `ji8i-4anb`) | Dynamic source, REST API | Departmental dropout (`desercion_media`) and repetition (`repitencia_secundaria`) per year | 462 raw rows (2011–2024) → 99 used (33 departments × 2022–2024) |

The heavy files (`.txt` datasets and the Power BI `.pbix`) are hosted in Kaggle because of GitHub's size limit:
https://www.kaggle.com/datasets/blackgrimoire/budget-to-brilliance  place the nine `Examen_Saber_11_<period>.txt` files in
`data/source/icfes/` before executing the project.

### 2.2 EDA / profiling findings (full detail: [README_2D](docs/batch/README_2D.md#8-data-profiling-findings))

| Finding | Effect on cleaning / transformation / quality rule |
|---|---|
| 5 delimiters and 3 encodings across the ICFES files | Streaming ingestion with Polars, per-period manifest (DQ-03, DQ-04) |
| 451,150 examinees (13.73%) with no school code = validantes | Mapped to `id_colegio = -1` / `id_departamento = -1`, kept in the fact, excluded from school comparisons and departmental correlations (DQ-11) |
| 81 spellings of department names | Normalised to the 33 DANE codes (T-06, DQ-26) |
| `URBANA` and `URBANO` for the same area | Harmonised (T-07, DQ-09) |
| 28 records with `punt_global = 0` | Flagged `puntaje_sospechoso`, excluded from KPIs (DQ-07) |
| 141 records without gender | Surrogate `-1`, excluded from the binary gap (DQ-08) |
| 7 defective MEN columns (100% null or copies of others) | Column whitelist (T-03, DQ-23) |
| MEN codes without zero padding (`'5'` instead of `'05'`) | Padded to DANE format (DQ-21) |

**How the EDA reaches the streaming component:** The event contract reuses the batch rules instead of inventing new ones: score ranges
(DQ-05), `puntaje_sospechoso ⇔ punt_global = 0` (DQ-07), DANE department domain (DQ-21), `No aplica ⇔ department '00'` (DQ-11), the closed
domains of sector/shift/area (DQ-09/10), and the metric eligibility rules (suspicious scores, validantes and unreported gender are
excluded exactly like in R1–R7).

---

## 3. System Architecture

```mermaid
flowchart TD
    subgraph SRC["Sources (unchanged)"]
        S1["ICFES Saber 11<br/>9 periods · 3.29M rows"]
        S2["MEN API (Socrata)<br/>dropout and repetition"]
    end
    subgraph BATCH["BATCH · Airflow 3.1.8"]
        A["extract → transform/integrate<br/>→ Great Expectations (3 gates) → load"]
    end
    DW[("MySQL 8 · icfes_dw<br/>fact_resultado_saber11<br/>fact_indicador_departamento + dims")]
    PBI["Historical dashboard<br/>Power BI (R1–R7)"]
    subgraph STREAM["STREAMING · Kafka (KRaft)"]
        P["Python producer<br/>simulated replay"]
        T[["saber11.resultados.v1<br/>3 partitions · key cod_dane_depto"]]
        C["Python consumer<br/>contract v1 + metrics"]
        D[["saber11.resultados.dlq.v1"]]
    end
    ST[("MySQL · stream_*<br/>accumulators + v_stream_* views")]
    G["Real-time dashboard<br/>Grafana (refresh 5 s)"]
    S1 --> A
    S2 --> A
    A --> DW
    DW --> PBI
    DW -->|"reads facts (deterministic sample)"| P
    P --> T --> C
    C -->|invalid| D
    C -->|"transaction + manual offset commit"| ST
    DW -.->|"MEN indicator snapshot"| C
    ST --> G
```
(Same diagram in [`docs/streaming_architecture.mmd`](docs/streaming_architecture.mmd).)

The architecture follows the recomendation show in the docuemnt of the assignment with two adaptations: the Streaming Metrics Store is a set of `stream_*` tables in the same MySQL server as the DW (one less component, still isolated by prefix, no foreign keys and a read-only account for Grafana) and a dead-letter topic receives the rejected events.

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 3.1.8 | Schedule and monitor the batch ETL |
| Event streaming | Apache Kafka 3.9 (KRaft, single broker) | Publish, retain and distribute events |
| Data sources | ICFES .txt files, MEN Socrata API | Historical and periodically updated data |
| Data storage | MySQL 8.0 (`icfes_dw`) | Dimensional model **and** processed streaming data |
| Quality | Great Expectations 1.24.0 · Pydantic 2 (event contract) | Batch gates · per-event validation |
| Transformation | Python (Polars/Pandas in batch) | Clean, integrate and transform |
| Visualization | Power BI (historical) · Grafana 11 (real time) | Historical and low-latency analytics |
| Versioning | Git, GitHub, Markdown | Version control and documentation |

---

## 4. Batch ETL Pipeline

Reused unchanged from the Second Delivery (`dags/budget_to_brilliance_dag.py`, DAG id `budget_to_brilliance_etl`, TaskFlow API).
Complete description: [README_2D](docs/batch/README_2D.md#16-airflow-dag-design-and-task-interfaces).

| Step | Task(s) | What happens |
|---|---|---|
| Prepare | `prepare_gx_project` | Initialises the Great Expectations context |
| Extract (ICFES) | `extract_primary` | Streams the 9 `.txt` files to raw Parquet + manifest |
| Extract (API) | `extract_api` | Paginated Socrata download (`$limit/$offset`), 3 retries with backoff, SHA-256 manifest |
| Validate raw | `validate_primary_raw` (Gate 1a) · `validate_api_raw` (Gate 1b) | Contract and domain checks on both raw sources |
| Transform / integrate | `transform_and_integrate` | T-01…T-14: cleaning, DANE codes, whitelist, department-year pre-aggregation |
| Validate prepared | `validate_prepared` (Gate 2) | Reconciliation, referential integrity, ranges, anomalies, 99-observation panel |
| Load | `load_dimensional_dw` | Full transactional refresh: dimensions → facts |
| Audit | `reconcile_dw` | Row counts against prepared data + 7 integrity queries |

Configuration: `schedule=None` (manual trigger; ICFES publishes semi-annually and MEN annually), `max_active_runs=1`,
`dagrun_timeout=3h`. Retries: 3 for transient network/API faults, 2 for transient DB faults, 0 for deterministic data or contract failures.

Interaction with the streaming component: the load does a full refresh (it empties the DW tables). The streaming tables are not part of that refresh and have no foreign keys to the DW, so the refresh can never block on them or wipe them (verified by tests). While the DW is empty, the consumer keeps its previous MEN snapshot.

---

## 5. Data Quality

### 5.1 Batch: Great Expectations

Three gates and 28 traceable rules (`DQ-01 … DQ-30`) defined once in `src/validation/dq_rules.py`; each expectation carries its `dq_rule_id` and severity in its metadata; results are serialised to `data/runs/<batch_id>/validation/<gate>.json`. Complete listing and explanation of DQ rules: [README_2D](docs/batch/README_2D.md#92-complete-data-quality-rules-catalog)

| Gate | When | Rules (examples) |
|---|---|---|
| 1a | raw ICFES | uniqueness, periods present, required columns, numeric scores, closed domains |
| 1b | raw API | service count = downloaded rows, DANE codes, rate bounds, conservation identity |
| 2 | prepared data | reconciliation, referential integrity, ranges, YoY anomalies, panel = 33 × 3 |

| Severity | Pipeline behaviour |
|---|---|
| **Critical** | Branch stops, `AirflowFailException`, downstream tasks `upstream_failed`, DW untouched/rolled back |
| Warning | Logged in the evidence JSON, execution continues |
| Informational | Recorded for monitoring only |

**Expected behaviour when a critical check fails:** the task fails in Airflow (red task, visible in Grid/Graph), no retries (deterministic failure), and nothing reaches the DW tables. It was demonstrated in the Second Delivery by injecting an out-of-domain department code (`'999'`) into the API batch (Reliability Test).

### 5.2 Streaming: event contract and invalid-event queue
Every event is validated by the consumer against contract v1. An invalid event never stops the stream: it is stored in `stream_eventos_invalidos` (with reason and Kafka position), forwarded to the DLQ topic, and counted in the Invalid indicator of the dashboard. The same `'999'` department defect used in Test B is one of the injectable invalid events, which links both levels of quality control.

---

## 6. Data Warehouse

### 6.1 Dimensional model (unchanged)
Fact constellation with two facts and conformed dimensions (full ERD in [`docs/resources/star_schema.mmd`](docs/resources/star_schema.mmd)).

```mermaid
erDiagram
    DIM_DEPARTAMENTO ||--o{ DIM_COLEGIO : ubica
    DIM_ANIO ||--o{ DIM_PERIODO : agrupa
    DIM_COLEGIO ||--o{ FACT_RESULTADO_SABER11 : describe
    DIM_GENERO  ||--o{ FACT_RESULTADO_SABER11 : describe
    DIM_PERIODO ||--o{ FACT_RESULTADO_SABER11 : describe
    DIM_DEPARTAMENTO ||--o{ FACT_INDICADOR_DEPARTAMENTO : describe
    DIM_ANIO ||--o{ FACT_INDICADOR_DEPARTAMENTO : describe

    DIM_DEPARTAMENTO {
        int id_departamento PK
        string cod_dane_depto UK "2 digitos, ej. 05"
        string nombre_departamento
    }

    DIM_ANIO {
        int id_anio PK
        int anio UK
    }

    DIM_COLEGIO {
        int id_colegio PK
        int id_departamento FK
        string cole_codigo_icfes
        string cole_nombre_establecimiento
        string cole_naturaleza
        string cole_jornada
        string cole_area_ubicacion
        string cole_depto_ubicacion
        string cole_mcpio_ubicacion
    }

    DIM_GENERO {
        int id_genero PK
        string estu_genero
        string genero_descripcion
    }

    DIM_PERIODO {
        int id_periodo PK
        int id_anio FK
        string periodo
        int anio
        int semestre
    }

    FACT_RESULTADO_SABER11 {
        string estu_consecutivo PK
        int id_periodo PK, FK
        int id_colegio FK
        int id_genero FK
        float punt_lectura_critica
        float punt_matematicas
        float punt_sociales_ciudadanas
        float punt_c_naturales
        float punt_ingles
        float punt_global
        boolean puntaje_sospechoso
    }

    FACT_INDICADOR_DEPARTAMENTO {
        int id_departamento PK, FK
        int id_anio PK, FK
        float desercion_media "porcentaje 0-100, no aditivo"
        float repitencia_secundaria "porcentaje 0-100, no aditivo"
    }
```

| Table | Grain/PK | Key |
|---|---|---|
| `fact_resultado_saber11` | one exam attempt of one student in one semester | `(estu_consecutivo, id_periodo)` |
| `fact_indicador_departamento` | one DANE department in one calendar year | `(id_departamento, id_anio)` |
| `dim_departamento` (conformed) | department (`-1` / `'00'` = validantes) | `id_departamento`, unique `cod_dane_depto` |
| `dim_anio` (conformed) | calendar year | `id_anio` |
| `dim_periodo`, `dim_colegio`, `dim_genero` | semester · school · gender | surrogate keys |

Loading strategy: idempotent DDL (`CREATE TABLE IF NOT EXISTS`) → empty in reverse dependency order → dimensions → facts by period →
reconciliation. Rates (`desercion_media`, `repitencia_secundaria`) are **non-additive**.

### 6.2 Streaming
Same database, prefix `stream_` / `v_stream_`. Idempotent DDL, **no foreign keys** towards the DW, **not** part of the DW full refresh.

| Object | Grain / purpose |
|---|---|
| `stream_eventos_procesados` | idempotency ledger: one row per applied `event_id` |
| `stream_eventos_invalidos` | rejected events; PK = Kafka `(partition, offset)` |
| `stream_metricas_departamento` | department × year accumulators (counts and sums) for M1, M2, M3 |
| `stream_metricas_genero` | department × year × measure × gender (global + 5 subjects) for M4 |
| `stream_estado` | single row: valid / invalid / duplicates / events without MEN reference / last event time |
| `stream_serie_tiempo` | time buckets (10 s): throughput, latency, cumulative national gaps |
| `stream_referencia_departamento`, `stream_referencia_indicador` | snapshot of the MEN indicators copied from the DW |
| `v_stream_departamento`, `v_stream_genero_depto`, `v_stream_genero_nacional`, `v_stream_riesgo(_base)`, `v_stream_spearman`, `v_stream_estado` | what Grafana reads (gaps, quadrants, Spearman, freshness) |

The accumulators store **sums and counts**, not averages, so incremental means are exact; gaps are computed in the views.
The streaming department × year grain is the same *integration grain* of the 2D, so MEN rates join without fan-out.

---

## 7. Batch Analytical Dashboard

Power BI connected **directly to the MySQL DW** (not to files). The `.pbix` is hosted in the Kaggle dataset above (size > 100 MB).
KPIs come from `sql/analytics/analytical_queries.sql`.

| Page | Requirements | Insight |
|---|---|---|
| 1  Baseline structural equity | R1, R2, R3 | Official vs. non-official gap, shift, top-10 urban–rural gaps |
| 2  Longitudinal departmental evolution | R4, R5 | Departmental ranking and gender gap over periods |
| 3  Integrated API analytics | R6 | Dropout vs. score by department and year |

![Page 1](docs/resources/Visualization-1.jpeg)
![Page 2](docs/resources/Visualization-2.jpeg)
![Page 3](docs/resources/Visualization-3.png)

R7 (repetition vs. score) is implemented in SQL (`analytical_queries.sql`) and in the **real-time dashboard** (M7); the Power BI file of
the 2D has no dedicated R7 page. R5 in Power BI plots global, mathematics and reading; the streaming M4 extends the gender gap to the five
subjects (§8.5).

---

## 8. Streaming Pipeline

### 8.1 Event generation strategy 
The exam attempts already exist in the DW: there is no live feed. The **producer replays** records of `fact_resultado_saber11`
(joined with its dimensions) as if they were arriving now. Every message has `event_type = saber11.result.replayed`.

* **Records, not KPIs.** Each message is a new exam attempt; the metrics are computed by the consumer, so no unchanged KPI value is ever republished.
* **Deterministic sample of one period.** Publishing 3.29M events at a visible rate would take days (at 20 events/s: ~46 hours). The producer selects
  `MOD(CRC32(estu_consecutivo), 100) < sample_pct` of a period (default 1%): the same command always replays the same events in the same order.
* **Rate:** `--rate` events per second (default 20) or `--burst` (no pacing) to simulate a peak.
* **Use periods 2022–2024** (e.g. `--period 2024-2`): the MEN indicators exist only for those years; for other periods the producer warns that M6/M7 will stay empty.
* **Experiment helpers** (they add extra messages, they never alter real ones): `--inject-invalid N` (8 kinds of contract violation) and `--inject-duplicates N`.

### 8.2 Topics

| Topic | Partitions | Retention | Replication | Key |
|---|---|---|---|---|
| `saber11.resultados.v1` | 3 | 7 days | 1 (single broker) | `cod_dane_depto` |
| `saber11.resultados.dlq.v1` | 1 | 30 days | 1 | original key |

Producer: `enable.idempotence=true`, `acks=all`, `lz4`. Consumer group `btb-stream-metrics`, `enable.auto.commit=false`, `auto.offset.reset=earliest`.

### 8.3 Event contract v1
Single source of truth: [`src/streaming/contract.py`](src/streaming/contract.py) (Pydantic, `strict`, extra fields forbidden); generated JSON Schema:
[`contracts/saber11_result_event.v1.schema.json`](contracts/saber11_result_event.v1.schema.json).

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string UUIDv5 | yes | **Event identifier**: deterministic from `estu_consecutivo\|periodo` |
| `event_type` | string | yes | const `saber11.result.replayed` (identifies the replay) |
| `event_version` | int | yes | const `1` |
| `event_ts` | datetime ISO-8601 with timezone | yes | **Timestamp**: UTC publication time |
| `cod_dane_depto` | string `^\d{2}$` | yes | **Event key** (also the Kafka message key); one of the 33 DANE codes or `'00'` |
| `estu_consecutivo` | string ≤ 30 | yes | |
| `periodo` | string `^\d{4}-[12]$` | yes | |
| `cole_naturaleza` | `OFICIAL` \| `NO OFICIAL` \| `No aplica` | yes | DQ-10 |
| `cole_jornada` | `COMPLETA` `UNICA` `MAÑANA` `TARDE` `NOCHE` `SABATINA` `No aplica` | yes | DQ-10 |
| `cole_area_ubicacion` | `URBANO` \| `RURAL` \| `No aplica` | yes | DQ-09 |
| `estu_genero` | `F` \| `M` \| `No informado` | yes | |
| `punt_lectura_critica`, `punt_matematicas`, `punt_sociales_ciudadanas`, `punt_c_naturales`, `punt_ingles` | number | **yes** | 0 – 100 (DQ-05) |
| `punt_global` | number | **yes** | 0 – 500 (DQ-05) |
| `puntaje_sospechoso` | bool | yes | `true` ⇔ `punt_global == 0` (DQ-07) |

Cross-field and message-level rules: DQ-21 (department in DANE list or `'00'`), DQ-11 (`cole_naturaleza = 'No aplica'` ⇔ department `'00'`),
`event_ts` must carry a timezone, and **the Kafka key must equal `cod_dane_depto`**. All scores are mandatory by team decision; the producer
checks every record with the contract *before* publishing and skips (and logs) any source row that would violate it.

Example of an event:
```json
{"event_id":"…","event_type":"saber11.result.replayed","event_version":1,"event_ts":"2026-11-17T12:00:00.123+00:00",
 "cod_dane_depto":"76","estu_consecutivo":"SB11…","periodo":"2024-2","cole_naturaleza":"OFICIAL","cole_jornada":"COMPLETA",
 "cole_area_ubicacion":"URBANO","estu_genero":"F","punt_lectura_critica":55.0,"punt_matematicas":52.0,
 "punt_sociales_ciudadanas":50.0,"punt_c_naturales":51.0,"punt_ingles":49.0,"punt_global":260.0,"puntaje_sospechoso":false}
```

### 8.4 Why the key is `cod_dane_depto`
Kafka guarantees order *within a partition*, so every event of a department is processed in order; it is the conformed key of the model
(`dim_departamento`) and the grain of R3/R4/R6/R7. `estu_consecutivo` would distribute better but gives no useful ordering. Trade-off: partition skew
(Bogotá, Antioquia, Valle carry far more events than Vaupés)  acceptable for 3 partitions and one consumer (see §12).

### 8.5 Streaming metrics

Eligibility inherited from the batch: suspicious scores never enter any score; official gap uses only `OFICIAL`/`NO OFICIAL`; urban–rural only
`URBANO`/`RURAL`; gender only `F`/`M`; *validantes* (department `'00'`) are excluded from the departmental views (R4, R6, R7).

| # | Metric | Definition | Requirement |
|---|---|---|---|
| M1 | Mean global score by department and year | `Σ punt_global / n` over non-suspicious events | SR1 · R4 |
| M2 | Official gap | mean(`NO OFICIAL`) − mean(`OFICIAL`) | SR2 · R1 |
| M3 | Urban–rural gap | mean(`URBANO`) − mean(`RURAL`) | SR2 · R3 |
| M4 | Gender gap (M − F) in the global score and the 5 subjects (lectura crítica, matemáticas, sociales y ciudadanas, ciencias naturales, inglés) | national and by department | SR2 · R5 |
| M5 | Stream quality | valid / invalid / duplicates / % invalid, events per second, end-to-end latency, seconds without data | SR3 |
| M6 | Score vs. dropout (MEN API) | live mean score against `desercion_media`, bivariate quadrant, Spearman ρ per year | SR4 · R6 |
| M7 | Score vs. repetition (MEN API) | live mean score against `repitencia_secundaria`, quadrant, Spearman ρ | SR4 · R7 |

**How the API enters the stream.** The consumer copies `fact_indicador_departamento` (+ `dim_departamento`) into `stream_referencia_*` at start-up and
every 5 minutes (`STREAM_REFERENCE_REFRESH_SECONDS`), and keeps the previous snapshot if the DW is empty during a batch full refresh. The join happens at the
integration grain (department × year) in `v_stream_riesgo`. The MEN rates are **not** embedded in each event: the 2D rejected duplicating macro-indicators on
student rows (README_2D §13.2). Events from a department-year with no MEN indicator are counted in `n_sin_referencia` instead of being hidden.

### 8.6 Consumer processing and delivery semantics
1. `consume()` a micro-batch (≤ 200 messages or 1 s).
2. Validate each message (contract + key rule). Invalid → `stream_eventos_invalidos` + DLQ topic.
3. In **one MySQL transaction**: ignore `event_id`s already in `stream_eventos_procesados` (duplicates), apply the increments to the accumulators, update the state and the time bucket.
4. Only **after** the MySQL commit: `consumer.commit()` of the offsets.

This is *at-least-once* from Kafka and *exactly-once* in effect on the metrics. A crash between steps 3 and 4 makes Kafka redeliver the batch; the ledger
turns it into duplicates that are not counted twice (tested). If MySQL fails the consumer exits without committing and Docker restarts it from the last committed offset.
The DLQ message is best-effort (it is sent after the MySQL commit); the durable record of a rejection is `stream_eventos_invalidos`.

### 8.7 Behaviour with an invalid event and with an interrupted consumer

| Scenario | How to run | Expected behaviour |
|---|---|---|
| Invalid events | `scripts/demo_experiments.sh invalid` | The 8 injected violations are rejected (each with its reason: pattern, range, missing field, wrong type, DQ-07, malformed JSON, key mismatch, extra field), logged once, sent to the DLQ; the valid events keep flowing; the *Invalid* indicator and the rejections table change in Grafana. The 5 injected duplicates are ignored (*Duplicates* indicator). |
| Consumer stopped 30 s | `scripts/demo_experiments.sh interruption` | Kafka retains the messages; consumer lag grows; the *Sin datos hace* indicator turns yellow/red. On restart the consumer resumes from the committed offset, catches up, and `n_validos` equals the number of distinct `event_id`s (no loss, no double count). |

**Limitations observed/expected:** if the consumer stays down longer than the topic retention (7 days) events are lost; with a single broker and replication 1 a broker failure loses data; the dashboard shows stale values (not errors) while the consumer is down, which is why freshness is a first-class indicator.

---

## 9. Real-Time Dashboard

### 9.1 Grafana

| Option | Real-time over MySQL | Reproducible from the repo | Decision |
|---|---|---|---|
| **Grafana** (MySQL datasource) | Yes: auto-refresh (1 s – 1 m) | Yes: datasource and dashboard are provisioned files | **Chosen** |

### 9.2 Consumer-to-dashboard integration and update interval

```
Kafka topic ──► consumer (micro-batch ≤ 1 s) ──► MySQL stream_* tables ──► v_stream_* views ──► Grafana (refresh 5 s)
```
* The consumer writes to MySQL after every micro-batch (≤ 1 s or 200 events).
* Grafana polls the views every **5 s** (`refresh: 5s`, selectable 1 s – 1 m) with the read-only account `grafana_ro` (SELECT on `stream_*` / `v_stream_*` only; it cannot read the DW).
* Worst-case latency from a message being consumed to appearing on screen: **≈ 6 s**. Provisioned from `grafana/provisioning/` and `grafana/dashboards/streaming.json` (generated by `scripts/build_grafana_dashboard.py`).

### 9.3 Dashboard content (`http://localhost:3000`, variable Año)



| Panels | Metric |
|---|---|
| 6 indicators: valid events, invalid (DLQ), duplicates, seconds without data, % invalid, latency (last minute) | M5 |
| Events per second (valid / invalid / duplicates) | M5 |
| Cumulative national gaps over time (official, urban–rural, gender) | M2, M3, M4 |
| Mean global score by department | M1 |
| Gender gap by measure (global + 5 subjects) | M4 |
| Gaps by department (table with N) | M2, M3 |
| Score vs. dropout · score vs. repetition (scatter) · risk quadrants · Spearman per year | M6, M7 |
| Last rejected events (reason and Kafka position) | M5 |

### 9.4 Difference from the historical dashboard

| | Historical (Power BI) | Real time (Grafana) |
|---|---|---|
| Reads | The whole DW (`fact_*`, `dim_*`) | Streaming accumulators `stream_*` |
| Changes | After each Airflow full refresh | After every micro-batch, visible within seconds |
| Shows | Final values of 2021-2…2025-2 | **Partial** values that converge as events arrive, plus flow health |
| Quality focus | Gates before loading | Invalid events, duplicates, freshness, latency |


---

## 10. Execution and Reproducibility

### 10.1 Requirements


### 10.2 Setup
```bash
git clone https://github.com/Bl4ck-Grimoire/Budget-To-Brilliance-3D.git # or also https://github.com/Johann0811/Budget-To-Brilliance-3D.git
cd Budget-To-Brilliance-Final
cp .env.example .env                       # defaults work for a local run; never commit .env
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate
# only install requirements.txt not requirements-streaming.txt 
pip install -r requirements.txt
# download the 9 ICFES .txt files from the Kaggle dataset (§2.1) into data/source/icfes/
docker compose build
docker compose up -d         # postgres, mysql, Airflow, Kafka, topics, stream tables, consumer, Grafana
docker compose ps                          # everything running/healthy; stream-init and kafka-init end "Exited (0)"
```
The first time you run docker compose up -d there will always be an ERROR in the streaming containers but this is controlled it will be solved after running the airflow dag (with the ICFES data inside its correponidng directory) after it finish running successfully you run again **docker compose up -d** and the streaming containers would be up and healthy. 

### 10.3 Batch (once, to fill the Data Warehouse)
1. Open Airflow http://localhost:8080 → DAG `budget_to_brilliance_etl` → unpause → **Trigger**.
2. Wait for all tasks to be green (`reconcile_dw` last). Verify:
```bash
docker exec -it btb_mysql mysql -uroot -pchangeme icfes_dw -e "SELECT COUNT(*) FROM fact_resultado_saber11; SELECT COUNT(*) FROM fact_indicador_departamento;"
```
(expected: 3,293,842 minus exact duplicates/quarantined rows, and 99.) Standalone alternative: `pip install -r requirements.txt && python -m src.run_pipeline`.

### 10.4 Streaming
```bash
# 1. the consumer is already running (docker compose up -d). Follow it:
docker compose logs -f stream-consumer
# 2. replay (simulation): 1% of period 2024-2 at 20 events/s
docker compose run --rm stream-producer --period 2024-2 --sample-pct 1 --rate 20
# 3. open Grafana: http://localhost:3000  (admin / admin by default, see .env) → dashboard "Saber 11  Streaming en tiempo real"
# optional: Kafka UI
docker compose --profile tools up -d kafka-ui        # http://localhost:8081
```
Producer options: `--period` (required) `--sample-pct 1..100` `--limit N` `--rate N` `--burst` `--inject-invalid N` (To inject invalid events to test the rejection and register of invalid events) `--inject-duplicates N` `--seed`.

Experiments: `scripts/demo_experiments.sh invalid` and `scripts/demo_experiments.sh interruption`. Clean slate between takes: `scripts/demo_reset.sh`.

### 10.5 Verify the whole system

| Check | Command / place | Expected |
|---|---|---|
| Topics exist | `docker compose logs kafka-init` | topic `saber11.resultados.v1` with 3 partitions |
| Events consumed | `docker compose logs stream-consumer` | `batch: N msgs -> N applied …` lines |
| Metrics updating | Grafana, or `SELECT * FROM v_stream_estado` | `n_validos` grows, `segundos_sin_datos` ≈ 0–6 |
| M6/M7 alive | Grafana scatter / `SELECT * FROM v_stream_riesgo` | 33 departments once the sample covers them |
| No loss, no duplicates | `SELECT n_validos, (SELECT COUNT(*) FROM stream_eventos_procesados) FROM stream_estado` | both numbers equal |

### 10.6 Automated tests
```bash
pip install -r requirements-streaming.txt pytest
python -m pytest tests/test_contract.py tests/test_metrics.py            # no database needed (42 tests)
# database tests need a THROWAWAY MySQL/MariaDB (they create and fill the database with synthetic data)
TEST_DATABASE_URL=mysql+pymysql://root:changeme@localhost:3307/icfes_dw_test python -m pytest tests    # 62 tests
```
Never point `TEST_DATABASE_URL` at the real Data Warehouse.

### 10.7 Configuration
`KAFKA_*` (bootstrap, topics, group) · `STREAM_MAX_BATCH`, `STREAM_FLUSH_SECONDS`, `STREAM_BUCKET_SECONDS` (**must match** the dashboard variable `bucket_seconds`, default 10),
`STREAM_REFERENCE_REFRESH_SECONDS` · `GRAFANA_DB_USER/PASSWORD` (read-only account) · `GRAFANA_ADMIN_*`. No credential is committed: `.env` is git-ignored and the defaults are for local use only.

### 10.8 Troubleshooting
* *The consumer says "no MEN indicators available"* → run the Airflow DAG first (the API branch fills `fact_indicador_departamento`); the snapshot refreshes within 5 min or on restart (`docker compose restart stream-consumer`).
* *M6/M7 empty* → the replayed period is outside 2022–2024.
* *Grafana panels without data* → the producer has not run yet, or the dashboard time range does not include the replay (time series use the last 15 min).
* *A scatter panel shows a configuration error* → the XY-chart panel options differ between Grafana versions; pick the X and Y fields manually in the panel editor (`desercion_media` / `repitencia_secundaria` on X) and re-export the JSON.

---

## 11. Functional Evidence

### 11.1 Batch (Second Delivery runs, reused pipeline)

| Evidence | Image |
|---|---|
| Test A  successful DAG execution | ![try-good](docs/resources/try-good.png) ![graph](docs/resources/try-good-graph.png) |
| Test B  controlled critical failure (`'999'` department code in the API batch) halts the pipeline | ![try-bad](docs/resources/try-bad.png) ![logs](docs/resources/try-bad-logs.png) ![graph](docs/resources/try-bad-graph.png) |
| Test C  safe re-run (idempotent full refresh) | ![rerun](docs/resources/rerun.png) ![graph](docs/resources/rerun-graph.png) |

### 11.2 Streaming: automated verification
[`docs/evidence/streaming/automated_tests.txt`](docs/evidence/streaming/automated_tests.txt) is the output of 62 passing tests, run against MariaDB 10.11 configured
with the default MySQL 8 `sql_mode` and a synthetic mini-DW with the exact shape of the real one. They verify, among others:

* the contract (all fields required, null/out-of-range/wrong-type/semantic violations rejected, `is_simulation` absent) and the 8 injectable invalid-event kinds;
* **equivalence with the batch**: the streaming M1 (department means), M2/M3 (national gaps) and M4 (gender gap in the 6 measures) equal the corresponding SQL computed directly on the fact table;
* M6/M7: MEN snapshot, quadrants and Spearman; events without MEN reference are counted, never silently dropped;
* idempotency: replaying the same events, duplicates inside a batch, and a crash between the MySQL commit and the offset commit give the same result as a clean run;
* invalid events are logged once (re-reading the same Kafka positions does not count them again), tombstones are rejected, a malformed message does not stop valid ones;
* the DW full refresh does not touch `stream_*` tables (no FK, not in the truncate list) and the MEN snapshot survives it;
* the read-only Grafana account can run every query of every dashboard panel (and cannot read DW tables).

These tests exercise the real consumer logic with simulated Kafka messages. They are not a substitute for the live run below.

### 11.3 Real-Time Dashboard

![Page 4](docs/evidence/Visualization-4.jpeg)
![Page 5](docs/evidence/Visualization-5.jpeg)
![Page 6](docs/evidence/Visualization-6.jpeg)
![Page 7](docs/evidence/Visualization-7.jpeg)

---

## 12. Engineering Decisions and Limitations

### 12.1 Decisions

| Decision | Why |
|---|---|
| Batch pipeline reused unchanged | The assignment asks to *extend* previous deliveries; the DAG, gates and DW are already validated |
| Replay of a deterministic sample, marked by `event_type` | Historical data cannot arrive "live"; determinism makes the demo reproducible and gives every group member the same events |
| Events are records, metrics are computed in the consumer | Avoids republishing unchanged KPIs (explicit requirement) and keeps the contract about facts |
| Contract = Pydantic model + exported JSON Schema; JSON, no Schema Registry | Enough for a "basic" contract, versioned in Git, reuses the batch domains; Avro/Protobuf + Registry is the production-grade alternative but adds a service |
| Key = `cod_dane_depto` | Per-department ordering and the conformed model key (§8.4) |
| All scores mandatory in the contract | Team decision: the source reports all of them; a missing score is a contract violation, not something to impute |
| Sums and counts in the accumulators | Exact incremental means; gaps computed in views |
| Idempotency ledger + manual commit after MySQL commit | Exactly-once effect on metrics on top of at-least-once delivery |
| `stream_*` tables in the same MySQL, no FKs, outside the DW refresh | One less component; the DW full refresh can neither block on nor wipe them |
| MEN snapshot copied by the consumer | Uses the API without embedding rates in events; survives the DW full refresh |
| Grafana with a read-only account | Native auto-refresh over MySQL, dashboard as code; least privilege |
| Streaming M4 covers global + 5 subjects | Gender gap requirement R5 is defined "global and by subject" |

### 12.2 Assumptions and known limitations
* **Simulation:** events are replayed history, not genuine new data; `event_ts` is publication time, not exam time.
* **Single broker, replication factor 1, PLAINTEXT, no authentication:** acceptable locally, not for production.
* **Retention 7 days:** a consumer down longer than that loses events; messages beyond retention cannot be replayed from Kafka (they can from the DW).
* **Partition skew** (key = department): a few departments carry most of the events.
* **MySQL as streaming serving store:** adequate for ~10²–10³ events/s; beyond that a dedicated store would be needed. The idempotency ledger grows without bound (a TTL/purge job would be needed for long runs).
* **Partial metrics:** with a 1% sample, small departments have few events; gaps are noisy early on (the dashboard shows N). Correlations are associations, not causality (as in R6/R7).
* **Quadrant thresholds** in the live view are the mean over the departments with data so far; if the Power BI quadrants use other cut-offs, align `v_stream_riesgo` with them.
* **The DLQ message is best-effort** (sent after the MySQL commit); the durable record is `stream_eventos_invalidos`.
* **Batch:** DAG is manually triggered (`schedule=None`), justified by the semi-annual/annual publication of the sources; DW load is a full refresh.
* **Verification status of this repository:** the streaming logic, the SQL objects, the contract and all dashboard queries are covered by automated tests. The Docker Compose stack (Kafka, Grafana provisioning, container wiring) and the Grafana dashboard rendering were not executed in the environment where the code was written (no Docker/Kafka available): YAML/JSON validity and image/tool versions must be confirmed on the first `docker compose up`.

### 12.3 Reliability and security considerations
* No credentials in the repository: `.env` is ignored, `.env.example` has local defaults only (change them if any port is exposed).
* Grafana never connects as root: `grafana_ro` has `SELECT` on `stream_*`/`v_stream_*` only.
* The consumer never commits offsets before MySQL commits; on a DB error it exits and resumes from the last committed offset.
* Invalid input cannot crash the consumer: any parsing/validation error is a *rejection*; payloads are truncated (4 kB) before being stored.
* Airflow runs with `SIMPLE_AUTH_MANAGER_ALL_ADMINS` (no login) for local use: do not expose port 8080.

---

## 13. Repository structure

```text
.
├── README.md                      # this document
├── docker-compose.yaml            # Airflow + MySQL (2D) + Kafka, consumer, producer(on demand), Grafana (new)
├── Dockerfile                     # Airflow image (2D)        ├── Dockerfile.streaming   # streaming image (new)
├── requirements.txt               # batch (2D)               ├── requirements-streaming.txt  # streaming (new)
├── .env.example  .gitignore
├── dags/budget_to_brilliance_dag.py          # Airflow DAG (2D)
├── gx/  src/{extract,transform,integrate,validation,load}/  src/{config,policy,utils,run_pipeline}.py   # batch (2D)
├── src/streaming/
│   ├── settings.py  contract.py  metrics.py  store.py  consumer.py  producer.py  init_db.py
├── contracts/saber11_result_event.v1.schema.json
├── sql/schema/{create_dw.sql, create_streaming.sql}   sql/analytics/analytical_queries.sql
├── grafana/{provisioning/, dashboards/streaming.json}
├── scripts/{build_grafana_dashboard.py, demo_experiments.sh, demo_reset.sh}
├── tests/                          # 62 tests (contract, metrics, consumer+SQL, dashboard queries)
└── docs/{batch/README_2D.md, resources/, streaming_architecture.mmd, evidence/streaming/}
```