# CO2 Energystack Industrial Migration Final Proposal

## Purpose

This proposal consolidates the current `CO_energystacck` Dash/Pandas application, the existing `bosch_co2ely_adb_batch` Databricks pipeline, and the migration analysis into one aligned target plan.

The target is an industrial analytics architecture where Databricks owns analytical data processing and the application owns user workflows, metadata, reporting interaction, and operational visibility.

## Executive Decision

Migrate `CO_energystacck` by evolving the current Databricks pipeline, not by replacing it.

Prioritize efficient execution and operational performance over strict medallion-layer purity. The medallion model should guide ownership and traceability, but it should not force extra passes, larger intermediate tables, or repeated parsing when a safe file-local transformation can be done earlier.

The existing pipeline already provides a strong industrial foundation:

- Polars/calamine Excel conversion without Spark Excel JAR dependency.
- Deterministic source-file UUIDs.
- Raw Parquet outputs for `filemeta`, `channel`, `timeseries`, and `statistics`.
- Auto Loader ingestion into bronze Delta tables.
- Long-format raw measurements.
- Initial silver publication, timestamp enrichment, DQ result tables, and gold dashboard/summary outputs.
- Databricks Asset Bundle jobs for converter, bronze, silver, DQ, gold, and end-to-end orchestration.

The remaining migration challenge is not basic ingestion. The critical work is to harden bronze lineage, introduce governed semantic silver, preserve local app calculation behavior with tests, and publish gold tables that fully replace local CSV/Feather reporting reads.

## Target Architecture

Use a split-plane architecture.

| Plane | Owner | Responsibilities |
| --- | --- | --- |
| Analytics plane | Databricks, Delta Lake, Unity Catalog, ADLS Gen2 | Source conversion, bronze ingestion, cleansing, mapping, DQ, KPI enrichment, aggregation, gold table publication, analytical lineage. |
| Application/control plane | FastAPI, Postgres, frontend, optional cache | Series metadata, files, worksheets, mappings, metric catalog, reports, tags, saved views, user permissions, pipeline status, app workflows. |
| Reporting UI | React/TypeScript long term, Dash acceptable temporarily | Standard reports, custom reports, chart interaction, filtering, exports, mapping review, data status visibility. |

Do not store full analytical time-series facts in Postgres. Postgres should hold transactional metadata and workflow state. Databricks Delta should remain the source of truth for raw, bronze, silver, and gold analytical data.

TimescaleDB and Redis are optional serving accelerators only after benchmark evidence shows Databricks SQL plus gold aggregation is insufficient.

## Current State Alignment

### Local Application

The current app combines too many responsibilities in one local process:

- Series lifecycle and metadata in JSON files.
- Excel upload and worksheet configuration in Dash callbacks.
- Column mappings in local JSON.
- Canonical metric definitions in `schema.csv`.
- Silver CSV generation through Pandas.
- KPI enrichment in `data_enrichment.py`.
- Aggregations as additional wide CSV files.
- Tags and report definitions from JSON.
- Feather and memory caches for performance.

This works for a local analytical tool, but it is not governed enough for multi-user, auditable, scalable production operation.

### Databricks Pipeline

The Databricks pipeline is already materially aligned with the target raw/bronze direction:

| Area | Current status | Migration reading |
| --- | --- | --- |
| Converter | Implemented | Keep and harden. It already outputs normalized raw contracts. |
| Raw Parquet contract | Implemented | Retain `filemeta/channel/timeseries/statistics` as the foundation. |
| Bronze Delta ingestion | Implemented | Continue Auto Loader pattern and formalize table contracts. |
| Long-format raw facts | Implemented | Preserve as the canonical direction. |
| Silver base/enriched | Partial | Useful bridge, but not yet governed semantic silver. |
| DQ | Partial | DQ exists, but must become canonical and evidence-preserving. |
| Gold | Partial | Current outputs are dashboard compatibility outputs, not the final report-serving model. |
| Metadata governance | Missing | Needs a control-plane metadata model and Databricks snapshots. |

## Target Data Model

### Converter And Bronze: Efficient Structural Normalization

The converter should do lightweight transformations that are deterministic, file-local, and cheaper while the Excel file is already parsed in memory. This is a performance optimization, not a semantic shortcut.

Appropriate converter responsibilities:

- Detect structural timestamp, date, time, and elapsed-time columns.
- Keep timestamp and elapsed-time fields as row context instead of unpivoting them as measurement signals.
- Unpivot only signal columns into long measurement rows.
- Preserve raw value strings, parsed numeric values, parse status, source row/sample offset, channel metadata, and file/worksheet lineage.
- Emit parse or structure issues that are known during conversion.

Responsibilities that should remain outside the converter:

- Mapping raw channels to governed canonical CO2 metrics.
- Business KPI formulas such as Energy Efficiency, Current density, Delta-p Anolyte, and SPCE.
- Plausibility rules tied to governed metric definitions.
- Tag classification, report shaping, and user-facing semantics.

### Bronze: Forensic Raw Contract

Bronze should remain append-oriented, lineage-heavy, close to source evidence, and efficient to consume downstream.

| Target table | Grain | Purpose | Current alignment |
| --- | --- | --- | --- |
| `bronze_file` or hardened `bronze_filemeta` | One row per source file | Source path, checksum/UUID, file metadata, ingestion status. | Partial: `bronze_filemeta` exists. |
| `bronze_worksheet` | One row per worksheet/group | Durable worksheet lineage, header/data-row settings, row counts. | Missing. |
| `bronze_channel` | One row per raw channel per worksheet | Raw channel ID, display name, unit, column index, and structural/signal role. | Partial: exists, needs worksheet linkage and role contract hardening. |
| `bronze_row_context` | One row per source data row | Timestamp, elapsed time, parse status, sample offset, source row, and file/worksheet lineage. | Missing. |
| `bronze_signal_long` or hardened `bronze_timeseries` | One row per raw signal measurement | Raw value, parsed value, sample offset, source row, signal channel, file/worksheet lineage. | Partial: long table exists with `value` and `value_str`, but currently includes structural columns unless filtered later. |
| `bronze_statistics` | One row per file/worksheet profile | Conversion statistics and profiling metadata. | Partial: exists. |
| `bronze_ingest_issue` | One row per parse/ingest issue | Unreadable sheets, missing headers, duplicate channels, parse failures, ambiguous time offsets. | Missing. |

Bronze must preserve enough evidence to explain every downstream null, clipped value, derived metric, and DQ warning, while avoiding unnecessary unpivoted rows for structural time columns.

### Silver: Governed Semantic Layer

Silver is the main migration workstream. It converts generic raw channels into canonical, typed, versioned metric facts.

| Target table | Purpose |
| --- | --- |
| `silver_dim_metric` | Governed metric catalog from former `schema.csv`: names, units, aliases, origins, plausibility rules, versions. |
| `silver_dim_series` | Series/test campaign metadata needed by analytical processing. |
| `silver_dim_mapping` | Active mapping snapshot from raw channel/channel name to canonical metric ID. |
| `silver_fact_measurement_long` | Canonical long measurement facts with `series_id`, `file_id`, `worksheet_id`, `sample_offset`, `elapsed_time_s`, `event_ts`, `metric_id`, value, unit, mapping version, and quality status. |
| `silver_fact_derived_metric_long` | Derived KPI facts, or the same fact table with `metric_origin = calculation`, formula version, and input completeness flags. |
| `silver_measurement_quality` | Row/metric-level DQ evidence: parse failure, invalid timestamp, plausibility rule outcome, clipping, missing input, divide-by-zero, duplicate key. |
| `silver_timeseries_wide_view` | Optional compatibility view only, not the canonical source of truth. |

The current `silver_fact_timeseries`, `silver_fact_timeseries_enriched`, and DQ result tables should be treated as bridges toward this governed model.

### Gold: Report-Serving Tables

Gold should be optimized for bounded UI/API queries and should preserve current report behavior.

| Target table | Purpose |
| --- | --- |
| `gold_timeseries_raw` | Fine-resolution chart-ready values by series, elapsed time, and metric. |
| `gold_timeseries_1min` | One-minute min/max/mean/count aggregation. |
| `gold_timeseries_15min` | Fifteen-minute min/max/mean/count aggregation. |
| `gold_metric_summary` | Series and metric summaries for KPI panels and catalog views. |
| `gold_report_metric_catalog` | Available metrics per series/report for selectors. |
| `gold_dq_status` | User-facing quality status and issue counts. |
| `gold_tagged_timeseries` | Optional materialized tag/facet view when tag filters are common. |

Gold must support the existing UI resolution behavior:

| Visible time span | Preferred data |
| --- | --- |
| Less than 10 hours | Raw gold data. |
| 10 to 100 hours | One-minute aggregation. |
| At least 100 hours | Fifteen-minute aggregation. |

The API should request only selected series, metrics, time windows, and resolutions. It should never ship whole wide series to the browser for normal chart interaction.

## Metadata And Control Plane

Move local JSON/CSV configuration into governed transactional metadata.

| Domain | Target owner | Notes |
| --- | --- | --- |
| Series and files | Postgres + API | Replace `series.json`; track lifecycle and file associations. |
| Worksheet config | Postgres + API | Store sheet, header row, first data row, and parsing directives. |
| Metric catalog | Postgres or governed repo config exported to Delta | Replace `schema.csv`; version aliases, units, origins, plausibility rules. |
| Mappings | Postgres + Delta snapshot | Version every mapping and stamp mapping version on facts. |
| Stack definitions | Postgres + Delta snapshot | Govern active area for current density. |
| Derived formulas | Repo-tested implementation plus formula metadata | Preserve formula versions and parity evidence. |
| Standard reports | Postgres or governed config imported by API | Preserve current report groupings. |
| Tags | Postgres + optional gold view | Preserve ordered category behavior and per-series ranges. |
| Pipeline runs | Postgres/API status plus Databricks run IDs | Separate orchestration state from analytical facts. |
| Saved views | Postgres | Store user report state and preferences. |

A staged metadata approach is acceptable: start with Delta/config snapshots required for silver mapping and formulas, then introduce full Postgres workflow support as the application plane matures.

## Preserved Business Logic

The following local app behavior must be preserved and proven with parity fixtures:

| Behavior | Target implementation |
| --- | --- |
| Raw channel mapping to canonical metrics | Governed silver mapping with mapping versions. |
| Date + Time to timestamp | Silver timestamp normalization with parse-status evidence. |
| Elapsed-time stitching across worksheets/files | Silver continuity rules backed by bronze source evidence. |
| Numeric coercion | Silver parsing with raw value, numeric value, and DQ flags. |
| Energy Efficiency | Databricks silver formula with formula version. |
| Delta-p Anolyte | Databricks silver formula with formula version. |
| Current density | Databricks silver formula using governed active area. |
| Single Pass Conversion Efficiency | Databricks silver formula using original constants and formula version. |
| Percent clipping and plausibility limits | Generic DQ/plausibility rule engine with original and adjusted values. |
| Time-bin aggregation | Gold raw/1min/15min tables. |
| Standard reports | API queries against gold report tables. |
| Custom reports | API-driven metric selection and bounded chart queries. |
| Tags/facets | Metadata-driven classification, materialized in gold when needed. |
| Exports | API/frontend export from selected gold slices. |

The frontend must not become the source of truth for scientific calculations, DQ decisions, official aggregation, or canonical analytical facts.

## Migration Plan

### Phase 0: Freeze Current Behavior

Objective: create a reliable migration baseline.

Deliverables:

- Preserve current local config files and representative source data as fixtures.
- Document expected outputs for standard reports, custom reports, aggregations, and KPI calculations.
- Create parity fixtures for at least one representative series.
- Record current formulas, constants, dynamic resolution thresholds, and tag behavior.

Exit criteria:

- Local app behavior is reproducible in tests or fixture outputs.
- Target pipeline changes can be compared against known expected results.

### Phase 1: Harden Raw And Bronze

Objective: make the current converter/bronze pipeline a durable forensic contract.

Deliverables:

- Formal bronze contract for file, worksheet, channel, timeseries, statistics, and ingest issue entities.
- Add durable worksheet lineage.
- Add source row number where possible; retain sample offset as processing lineage.
- Add consistent `run_id` or `converter_run_id` to raw/bronze outputs.
- Add `bronze_ingest_issue` for parse and conversion problems.
- Distinguish converter tracking from analytical bronze.
- Keep integration-test paths isolated from production locations.

Exit criteria:

- Every bronze measurement can be traced to source file, worksheet/group, row/sample, channel, and pipeline run.
- Parse and conversion issues are materialized instead of hidden in logs.
- Raw/bronze E2E runs are executable in Databricks test infrastructure.

### Phase 2: Introduce Governed Silver

Objective: move local Pandas semantics into Databricks as governed canonical facts.

Deliverables:

- `silver_dim_metric`, `silver_dim_series`, and `silver_dim_mapping` snapshots.
- `silver_fact_measurement_long` built from bronze and mapping metadata.
- Formula implementation for Energy Efficiency, Delta-p Anolyte, Current density, and SPCE.
- Formula versions stamped on derived facts.
- Canonical `silver_measurement_quality` model.
- Generic plausibility rule handling beyond percentage clipping.
- Compatibility views only where needed for transitional consumers.

Exit criteria:

- KPI outputs match local app fixture outputs within agreed numeric tolerances.
- Mapping version and formula version are traceable from facts.
- DQ outcomes are queryable and evidence-preserving.
- Silver can feed raw/1min/15min gold without local CSV logic.

### Phase 3: Publish Gold Reporting Contracts

Objective: replace local CSV/Feather report reads with query-optimized gold tables.

Deliverables:

- `gold_timeseries_raw`, `gold_timeseries_1min`, and `gold_timeseries_15min`.
- Min/max/mean/count aggregation with quality counts.
- `gold_metric_summary`, `gold_report_metric_catalog`, and `gold_dq_status`.
- Optional `gold_tagged_timeseries` for common tag filters/facets.
- Query patterns optimized for `series_id`, `metric_id`, and elapsed time windows.

Exit criteria:

- Standard report queries can be served from gold.
- Dynamic raw/1min/15min resolution behavior matches the local app.
- Gold outputs are narrow, bounded, and API-ready.

### Phase 4: Build Application/API Slice

Objective: prove the final user-facing architecture without overbuilding infrastructure.

Deliverables:

- Minimal metadata schema for series, files, worksheets, metrics, mappings, reports, tags, and pipeline status.
- FastAPI endpoints for metadata and standard report chart queries.
- Databricks gold query service with point-budget enforcement.
- One preserved Standard Report view, preferably Voltage, backed by gold data.
- JSON response first; Arrow IPC only after payload/latency measurements justify it.

Exit criteria:

- User can select a series and report.
- App reads prepared gold data, not local silver files.
- Chart output matches the local report fixture.
- Browser handles zoom, hover, legend toggles, and axis display interactions without recalculating official data.

### Phase 5: Modernize And Optimize

Objective: complete the production application and tune only where evidence requires it.

Deliverables:

- React/TypeScript frontend or a staged Dash transition if delivery constraints require it.
- Custom Reports, Mapping Review, Tags, Saved Views, Series Catalog, and Data Status views.
- Authentication, authorization, audit events, and operational monitoring.
- Benchmark Databricks SQL latency, payload sizes, and repeated chart windows.
- Add Redis or Timescale hot cache only if benchmarks prove need.

Exit criteria:

- Local CSV/Feather reporting path is retired.
- Users can inspect pipeline status and DQ status from the app.
- Performance targets are met with the simplest proven serving stack.

## Recommended First Implementation Slice

Build the smallest slice that proves the final architecture end to end:

1. Harden current bronze contract enough to preserve file, worksheet/group, channel, sample, raw value, parsed value, and run lineage.
2. Split converter output into row context for timestamp/elapsed-time fields and signal-only long measurements.
3. Create minimal metric and mapping snapshots for one representative series.
4. Build canonical silver measurement facts for that series.
5. Port the four local KPI formulas into silver and verify parity against local outputs.
6. Publish raw, one-minute, and fifteen-minute gold tables for the selected series.
7. Add a simple API query for one Standard Report, such as Voltage.
8. Validate the returned data against current Dash report behavior.

This slice avoids premature Timescale, Redis, Arrow Flight SQL, unnecessary medallion handoffs, and a full frontend rewrite while proving the essential migration architecture.

## Acceptance Gates

Do not move to broad rollout until these gates are met:

| Gate | Required evidence |
| --- | --- |
| Bronze lineage | Measurement rows trace to source file, worksheet/group, row/sample, channel, and run. |
| DQ evidence | Parse failures, invalid timestamps, clipping, missing inputs, and duplicate keys are queryable. |
| Formula parity | Derived KPI values match local app fixtures within agreed tolerance. |
| Report parity | At least one Standard Report matches local Dash output for the same series/time window. |
| Gold serving | API can query bounded raw/1min/15min slices by series, metric, and elapsed time. |
| Metadata traceability | Facts include mapping and formula version references where applicable. |
| Operational isolation | Integration-test runs cannot write into production table locations. |

## Main Risks And Mitigations

| Risk | Mitigation |
| --- | --- |
| Existing Excel files vary in header and worksheet structure. | Persist worksheet/header configuration and preserve issue rows for parse ambiguity. |
| Mapping changes invalidate historical interpretation. | Version mappings and stamp processed facts with mapping version. |
| KPI formulas drift from the local app. | Create parity fixtures before changing formula implementations. |
| DQ deletes evidence needed by engineers. | Prefer evidence-preserving quality tables and clean views over destructive cleanup. |
| Gold remains too wide or too app-specific. | Keep canonical silver long; create narrow gold tables for selected chart/query patterns. |
| Application queries become slow. | Use gold aggregations, point budgets, Databricks SQL caching, and only then optional Redis/Timescale. |
| Metadata platform delays pipeline progress. | Start with governed snapshots needed by silver, then mature into Postgres-backed workflows. |

## Open Decisions

1. Should canonical metric definitions be mastered in Postgres first, or in governed repo config exported to both Postgres and Delta during the transition?
2. Should worksheet/header configuration drive the converter directly, or should the converter remain generic and semantic correction happen in silver?
3. What numeric tolerance is acceptable for KPI parity tests?
4. What chart latency target should determine whether Redis or Timescale is needed?
5. Which user roles are required for mapping edits, pipeline reruns, report exports, and admin workflows?
6. Which report should be the first official parity target: Voltage, Energy Efficiency, or a broader multi-axis report?

## Final Recommendation

Proceed with an evolutionary migration centered on the existing Databricks pipeline.

The correct order is:

1. Harden forensic bronze.
2. Build governed semantic silver.
3. Publish report-serving gold.
4. Add API/frontend workflows over gold and metadata.
5. Optimize serving only after benchmark evidence.

This path keeps the working converter and bronze foundation, moves scientific processing out of the UI, makes DQ and lineage auditable, and gives the reporting application a scalable data contract without copying analytical time-series into an application database.
