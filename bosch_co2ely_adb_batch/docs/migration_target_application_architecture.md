# CO2 Energystack Migration Target Architecture

## Purpose

This document describes a recommended target architecture for migrating the local `CO_energystacck` Dash application into a professional analytics application backed by Databricks pipelines.

The main design principle is to separate the platform into two planes:

1. **Analytics plane**: Databricks, Delta Lake, Unity Catalog, ADLS Gen2. This plane owns large time-series data, batch processing, enrichment, aggregations, and data quality.
2. **Application serving/control plane**: application API, Postgres, optional cache, authentication, and frontend. This plane owns user workflows, app metadata, mappings, report definitions, tags, saved views, and job/status orchestration.

This avoids turning Databricks into an application database, while also avoiding copying full analytical time-series data into Postgres.

## Current Local Application Responsibilities

The local `CO_energystacck` app currently combines several responsibilities in one process and local file tree:

| Current area | Local implementation | Responsibility |
| --- | --- | --- |
| Raw file onboarding | `data/bronze/*.xlsx`, Dash upload controls | Add Excel files to a series |
| Series metadata | `config/series.json`, `SeriesDataManager` | Series definitions, stack type, files, worksheets, header rows |
| Schema/catalog | `config/schema.csv` | Canonical metric names, units, aliases, origins, plausibility rules, channel IDs |
| Mapping | `data/bronze/*_mapping.json` | Map raw file columns to canonical schema metrics |
| Enrichment | `backend/data_enrichment.py` | Energy Efficiency, Current Density, Delta-p Anolyte, SPCE, plausibility clipping |
| Aggregation | `aggregate_timeseries()` | Time-bin min/max/mean outputs for interactive plotting |
| Standard reports | `config/standard_reports.json` | Curated report definitions and axis groupings |
| Tags | `config/tags.json`, `TagManager` | Dynamic tag definitions and per-series ranges |
| Visualization | Dash + Plotly | Standard reports, custom reports, slicers, exports |
| Cache | Feather files and server memory | Fast local reloads |

In the target architecture, these responsibilities should be split between Databricks, Postgres, and the application API.

## Recommended Target Tech Stack

### Frontend

Recommended long-term:

- React + TypeScript, preferably Next.js or Vite depending on hosting constraints.
- Plotly.js for continuity with the current Dash/Plotly behavior, or Apache ECharts for large interactive chart workloads.
- Microsoft Entra ID authentication via MSAL.
- Component library: Fluent UI, MUI, or a Bosch-approved design system if available.

Pragmatic transition option:

- Keep Dash temporarily while moving data access and metadata persistence behind an API.
- Replace local JSON/CSV dependencies first, then modernize the UI later.

### Application API

Recommended:

- Python FastAPI.
- SQLAlchemy 2.x for Postgres access.
- Alembic for schema migrations.
- Pydantic for request/response contracts.
- Databricks SQL Connector for Python for gold table queries.
- Azure SDK for file upload pre-signed URL generation or managed storage access.

Main API responsibilities:

- Series, file, worksheet, mapping, report, tag, and saved-view CRUD.
- Pipeline run trigger/status endpoints.
- Query planning for chart requests.
- Enforcement of user permissions and app workflow rules.
- Read-through cache coordination for expensive chart queries.

### Serving Database

Recommended default:

- Azure Database for PostgreSQL Flexible Server.

Use Postgres for small, transactional, multi-user application state:

- Series metadata.
- File-to-series associations.
- Worksheet/header configuration.
- Canonical metric catalog.
- Channel mappings and mapping versions.
- Stack definitions.
- Report definitions.
- Tag definitions and tag ranges.
- Saved custom reports and user preferences.
- Pipeline run and file processing status snapshots.
- App audit events.

Do **not** store the full analytical time-series facts in Postgres by default.

### TimescaleDB Extension

Recommendation:

- Do not require TimescaleDB for phase 1.
- Consider TimescaleDB only as an optional hot serving cache for selected, recent, or heavily used gold time-series slices.

Good Timescale use cases:

- Low-latency chart windows where Databricks SQL is too slow or too expensive.
- Curated dashboard metrics with continuous aggregates.
- Operational monitoring views over recent test runs.
- Compressed retention of a small subset of gold data.

Avoid using Timescale as the canonical store for all raw/silver/gold measurements. Databricks Delta should remain the source of truth for analytical time-series.

### Analytics Platform

Recommended:

- Azure Databricks Workflows using Declarative Automation Bundles.
- ADLS Gen2 for raw files and external table storage.
- Unity Catalog for catalogs, schemas, permissions, lineage, and external locations.
- Delta Lake tables for bronze, silver, gold, DQ, and aggregation outputs.
- Databricks SQL Warehouse for application read queries against gold tables.
- Databricks Jobs for conversion, bronze ingestion, silver enrichment, DQ, and gold publishing.

### Storage

Recommended:

- ADLS Gen2 container `co2elyd-data`.
- Raw source prefix: `raw_data/`.
- Converter output prefix: `parquet_raw/`.
- Delta table external locations for bronze/silver/gold.
- Optional quarantine prefix for rejected/corrupt files.

### Cache

Recommended optional components:

- Azure Cache for Redis for app-level query/result cache.
- Browser-side cache for UI state and recent chart windows.
- Databricks SQL result cache for repeated analytical queries.

Redis is useful if the same chart windows are repeatedly requested by many users. It is not required on day one.

### Identity, Security, and Secrets

Recommended:

- Microsoft Entra ID for user authentication.
- Application roles mapped from Entra groups.
- Managed Identity for Azure resource access where possible.
- Azure Key Vault for database credentials, Databricks tokens if needed, and other secrets.
- Unity Catalog permissions for data access.
- Postgres row-level or API-level authorization for app metadata.

### Observability

Recommended:

- Azure Application Insights for frontend/API telemetry.
- Azure Monitor dashboards and alerts.
- Databricks job run history and audit logs.
- Structured API logs with correlation IDs.
- Postgres audit table for user actions that change app state.

### CI/CD and Infrastructure

Recommended:

- Azure DevOps pipelines, matching the existing repository direction.
- Databricks Asset Bundles for Databricks jobs and resources.
- Terraform or Bicep for Azure resources.
- Alembic migrations for Postgres schema evolution.
- Unit tests for mapping/enrichment logic.
- Integration tests using a small fixture dataset and Databricks integration test targets.

## Target Data Flow

### 1. Onboarding Flow

1. User creates or updates a series in the application.
2. App API writes series metadata to Postgres.
3. User uploads Excel files through the frontend.
4. App API stores file metadata in Postgres and uploads the file to ADLS `raw_data/`.
5. User configures worksheets, header rows, first data rows, and mappings.
6. App API versions and stores mapping configuration in Postgres.
7. App API triggers the Databricks pipeline or marks the series for the next scheduled run.

### 2. Databricks Processing Flow

1. Converter discovers new raw files from ADLS.
2. Converter parses Excel with Polars/calamine and writes Parquet outputs: filemeta, channel, timeseries, statistics.
3. Bronze Auto Loader ingests converter Parquet outputs into Delta tables.
4. Silver base republishes bronze tables as warehouse-style dim/fact tables.
5. Silver semantic mapping joins channel metadata to canonical metric definitions from Postgres-exported dimensions or Delta config tables.
6. Silver enrichment calculates the same KPIs currently calculated in the local app.
7. DQ validates timestamp parseability, uniqueness, plausibility limits, and expected metric coverage.
8. Gold publishes dashboard-ready views and aggregation tables.

### 3. Application Read Flow

1. Frontend requests a report, chart, or export.
2. API reads app metadata from Postgres.
3. API builds a query against Databricks gold tables.
4. API optionally checks Redis for a cached result.
5. API returns chart-ready data or an export file.
6. Frontend renders standard/custom report views.

## Databricks Table Recommendations

### Bronze

| Table | Purpose |
| --- | --- |
| `bronze_filemeta` | One row per source file |
| `bronze_channel` | Channel metadata from file headers |
| `bronze_timeseries` | Long-format raw measurements |
| `bronze_statistics` | Per-file/per-sheet conversion statistics |

These tables already align with the current converter design.

### Silver

| Table | Purpose |
| --- | --- |
| `silver_dim_filemeta` | Clean file metadata |
| `silver_dim_channel` | Clean channel metadata |
| `silver_fact_timeseries` | Generic long-format fact table |
| `silver_fact_timeseries_enriched` | Long-format fact table with event time, elapsed time, metric metadata, DQ flags |
| `silver_dim_metric` | Canonical metric catalog exported from app metadata or maintained as Delta config |
| `silver_dim_channel_mapping` | Versioned mapping from raw channel/channel_name to canonical metric |
| `silver_fact_timeseries_wide` | Optional wide semantic table for compatibility with existing report logic |

### Gold

| Table | Purpose |
| --- | --- |
| `gold_timeseries_dashboard_raw` | Chart-ready raw resolution data |
| `gold_timeseries_dashboard_1min` | 1-minute min/max/mean aggregation |
| `gold_timeseries_dashboard_15min` | 15-minute min/max/mean aggregation |
| `gold_summary_statistics` | Series/file/run KPI summaries |
| `gold_report_metric_catalog` | Available metrics for frontend selectors |
| `gold_dq_status` | User-facing quality status by series/file |

## Serving Database Table Recommendations

### Core metadata

| Table | Purpose |
| --- | --- |
| `app_user` | User profile and identity mapping |
| `series` | Series master record |
| `series_file` | Files associated with a series |
| `series_worksheet` | Worksheet config, header row, first data row |
| `stack_definition` | Stack type and active area |
| `metric_definition` | Canonical metrics from former `schema.csv` |
| `channel_mapping_version` | Mapping version per series/file/config |
| `channel_mapping_entry` | Raw file/channel to canonical metric entries |

### Reporting metadata

| Table | Purpose |
| --- | --- |
| `standard_report` | Report definition header |
| `standard_report_axis_metric` | Metrics assigned to y1/y2 axes |
| `saved_custom_report` | User-created report definitions |
| `saved_view` | Persisted filters, time windows, axis ranges, layout state |

### Tag metadata

| Table | Purpose |
| --- | --- |
| `tag_definition` | Tag type, source metric, divisor, default category |
| `tag_category` | Ordered category labels |
| `series_tag_range` | Per-series category ranges |

### Workflow/status metadata

| Table | Purpose |
| --- | --- |
| `pipeline_run` | Databricks run IDs and status snapshots |
| `file_processing_status` | Per-file lifecycle status |
| `dq_status_snapshot` | User-facing DQ summary cache |
| `app_audit_event` | User actions that mutate app state |

## Timescale Optional Hot Cache

If performance testing shows Databricks SQL is not responsive enough for repeated interactive chart reads, add a separate Timescale hypertable for a curated subset of gold data.

Example hypertable:

| Column | Notes |
| --- | --- |
| `event_ts` | Hypertable time column |
| `series_id` | App series ID |
| `metric_id` | Canonical metric ID |
| `resolution` | raw, 1min, 15min |
| `value` | Numeric value |
| `value_min` | Aggregated min |
| `value_max` | Aggregated max |
| `value_mean` | Aggregated mean |
| `source_gold_table` | Traceability |
| `synced_at` | Cache freshness |

Rules for Timescale:

- Keep it derived and rebuildable.
- Store only selected metrics/resolutions.
- Keep Databricks as the analytical source of truth.
- Use retention and compression policies.
- Do not put mapping, enrichment, or DQ logic exclusively in Timescale.

## Feature Preservation Mapping

| Local feature | Target owner | Notes |
| --- | --- | --- |
| Series CRUD | API + Postgres | Replace `series.json` |
| File upload | Frontend + API + ADLS | API persists metadata and writes to ADLS |
| Worksheet/header config | API + Postgres | Feed into converter or semantic parsing stage |
| Column mapping | API + Postgres + Silver mapping table | Version mappings; publish to Databricks |
| Stack active area | Postgres + Silver enrichment | Needed for Current density |
| Enrichment formulas | Databricks Silver | Preserve calculations from local app |
| Plausibility limits | Databricks DQ/Silver | Move from schema CSV to rules/dim_metric |
| Standard reports | Postgres or repo config + API | App reads via API |
| Custom reports | Frontend + API + Databricks SQL | Query gold long/wide tables |
| Tags/facets/legend grouping | Postgres config + Gold tagged view | Can also be applied app-side initially |
| Export CSV/HTML/image | Frontend/API | Query gold, then render/export |
| Feather cache | Remove | Replace with Databricks/Redis/app cache |

## Key Design Decisions

### Decision 1: Keep full time-series in Databricks

Reason:

- High-volume analytical facts fit Delta Lake better than Postgres.
- Databricks provides scalable joins, aggregations, DQ, lineage, and governance.
- Avoid duplicating large datasets into an operational database.

### Decision 2: Use Postgres for app metadata

Reason:

- App metadata is transactional and multi-user.
- JSON files are not safe enough for production concurrent edits.
- Postgres supports versioning, constraints, audit trails, backups, migrations, and operational tooling.

### Decision 3: Treat Timescale as optional

Reason:

- It may be useful for low-latency curated chart serving.
- It is not necessary for the first professional version.
- It adds operational cost and data synchronization complexity.

### Decision 4: Introduce a semantic silver layer

Reason:

- Current converter output is intentionally generic.
- Existing reports expect canonical metric names like `Stack Voltage`, `Current density`, and `Energy Efficiency`.
- The bridge between generic channels and app semantics must be explicit, testable, and versioned.

## Migration Phases

### Phase 0: Stabilize Current Understanding

- Freeze current local app behavior and config files as migration fixtures.
- Export existing `series.json`, mapping JSON files, `schema.csv`, `tags.json`, `standard_reports.json`, and `stack_definitions.json`.
- Create golden sample datasets for PoCII/PoCIII/PoCIV/PoCVI.

### Phase 1: Metadata Serving Foundation

- Create Postgres schema.
- Import local JSON/CSV config into Postgres.
- Build API endpoints for series, files, mappings, tags, and reports.
- Keep frontend close to current behavior while replacing local file reads.

### Phase 2: Databricks Semantic Silver

- Publish metric definitions and mappings into Databricks-accessible Delta config tables.
- Add channel-to-canonical metric mapping.
- Add enrichment formulas and plausibility handling.
- Add DQ checks for required metrics and plausible ranges.

### Phase 3: Gold Dashboard Tables

- Build raw, 1-minute, and 15-minute dashboard-ready gold tables.
- Build summary KPI tables.
- Adapt frontend/API report queries to use gold tables.
- Validate report parity against local app outputs.

### Phase 4: Application Modernization

- Decide whether to keep Dash or move to React/TypeScript.
- Add saved views, sharing, permissions, and audit trail.
- Add operational monitoring and user-facing pipeline status.

### Phase 5: Performance Optimization

- Benchmark Databricks SQL query latency and cost.
- Add Redis caching if repeated windows are common.
- Add Timescale hot cache only if Databricks SQL plus cache is insufficient.

## Main Risks And Mitigations

| Risk | Mitigation |
| --- | --- |
| Existing Excel files do not follow one header pattern | Persist per-series worksheet/header config and pass it into parsing or semantic mapping |
| Gold scripts expect wide columns but silver is long-format | Add explicit long-to-wide dashboard view or update gold code to consume long semantic facts |
| Mapping changes over time invalidate historical interpretation | Version mappings and store mapping version on processed facts |
| App becomes slow when querying Databricks directly | Use gold aggregations, SQL Warehouse, query limits, Redis cache, and optional Timescale hot cache |
| Duplicate logic between app and pipeline | Move enrichment and DQ logic into shared tested modules or Databricks silver jobs |
| Users edit mappings while pipeline runs | Use mapping versions and run-level snapshots |
| Local tags are applied in memory only | Persist tag definitions/ranges and materialize tagged gold view if needed |

## Open Decisions

1. Should the first professional UI remain Dash, or should it move directly to React/TypeScript?
2. Should worksheet/header configuration influence the converter directly, or should raw conversion stay generic and semantic correction happen later?
3. Should canonical metric definitions be mastered in Postgres and exported to Delta, or mastered in repo-controlled config and imported into both?
4. What chart latency target is acceptable for interactive reports?
5. Which metrics and resolutions, if any, deserve a Timescale hot cache?
6. What is the required audit/compliance level for mapping changes and report exports?

## Recommended First Implementation Slice

The most valuable first slice is:

1. Postgres schema for series, files, worksheets, metric definitions, mappings, reports, and tags.
2. Import existing local config into Postgres.
3. Databricks silver semantic mapping from generic channel facts to canonical metric facts.
4. Reproduce local enrichment formulas in silver.
5. Gold raw/1min/15min dashboard tables.
6. API endpoint for standard report chart data.
7. Parity test comparing one local report output against the new gold query output.

This slice proves the architecture without prematurely adding Timescale, Redis, or a full frontend rewrite.
