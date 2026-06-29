# CO2 Energystack Data Processing And Reporting Scope

## Purpose

This document defines the business and functional scope for migrating the current local `CO_energystacck` application into a production analytics application backed by Databricks pipelines and a modern reporting frontend.

The key product decision is:

- Data onboarding, mapping, cleansing, enrichment, data quality, aggregation, and dashboard-serving preparation move into backend services and Databricks data pipelines.
- The application UI preserves only reporting, exploration, filtering, exporting, user workflow visibility, and configuration workflows.
- The application must not perform core scientific or analytical data processing in the browser or in UI callbacks.

## Target Product Boundary

### Data Processing Plane

Owned by Databricks pipelines and backend services.

Responsibilities:

- Raw file discovery and ingestion.
- Excel parsing and conversion.
- File metadata extraction.
- Worksheet and header interpretation.
- Channel catalog creation.
- Canonical metric mapping.
- Data cleansing and numeric coercion.
- Timestamp and elapsed-time normalization.
- Derived KPI calculation.
- Plausibility and DQ validation.
- Time-bin aggregation.
- Gold table publication for reporting.
- Optional sync to a high-performance chart serving store if required.

### Application Reporting Plane

Owned by React/TypeScript frontend and FastAPI backend.

Responsibilities:

- Standard report viewing.
- Custom report building.
- Series and metric selection.
- Time slicing and range interaction.
- Axis range overrides.
- Tag/filter/facet/legend interaction.
- Chart export and data export.
- Saved views and saved custom reports.
- Pipeline status visibility.
- User authentication and authorization.
- Metadata management workflows.

## Original App Feature Disposition

| Original feature | Current location | Target owner | Migration decision |
| --- | --- | --- | --- |
| Excel file upload | Dash Series Management | API + ADLS | Preserve workflow, move storage to ADLS |
| Series create/delete/edit | `SeriesDataManager`, `series.json` | API + Postgres | Preserve as metadata workflow |
| File-to-series association | `series.json` | API + Postgres | Preserve as metadata |
| Worksheet selection | `series.json` | API + Postgres + pipeline config | Preserve; pipeline consumes config |
| Header row and first data row | `detect_header_and_data_row`, `series.json` | Pipeline config + parser | Move to data processing logic |
| Column mapping UI | Series Management DataTable | App UI + API + Postgres | Preserve UI, processing uses versioned mapping |
| Mapping application | `apply_mapping()` | Databricks Silver | Move to pipeline |
| Date + Time to Timestamp | `apply_mapping()` | Databricks Silver | Move to pipeline |
| Time stitching across worksheets/files | `load_and_concat_files_and_worksheets()` | Databricks Silver | Move to pipeline |
| Numeric coercion | `DataEnrichment.clean_data()` | Databricks Silver | Move to pipeline |
| Energy Efficiency | `DataEnrichment.add_energy_efficiency()` | Databricks Silver | Move to pipeline |
| Current Density | `DataEnrichment.add_current_density()` | Databricks Silver | Move to pipeline |
| Delta-p Anolyte | `DataEnrichment.add_delta_p_anolyte()` | Databricks Silver | Move to pipeline |
| Single Pass Conversion Efficiency | `DataEnrichment.add_single_pass_conversion_efficiency()` | Databricks Silver | Move to pipeline |
| Percent clipping to 0-100 | `apply_plausibility_limits()` | Databricks DQ/Silver | Move to pipeline/DQ |
| Time-bin aggregation | `aggregate_timeseries()` | Databricks Gold | Move to pipeline |
| Dynamic raw/1min/15min resolution | Dash helpers | Gold tables + frontend selection | Preserve behavior, backend prepares data |
| Standard Reports | `standard_reports.json`, Dash tab | App reporting UI + API | Preserve on app level |
| Custom Reports | Dash tab | App reporting UI + API | Preserve on app level |
| Tag filtering | `tags.json`, `TagManager` | Postgres metadata + gold tagged view/API | Preserve reporting behavior; classification can be materialized |
| Facets and legend grouping | Custom Reports tab | Frontend | Preserve on app level |
| Save graph | Dash export helpers | Frontend/API | Preserve on app level |
| Download selected data | `download_selected_data()` | API export endpoint | Preserve on app level |
| Local Feather cache | `cache/` | Remove | Replace with Databricks SQL cache, API cache, browser cache, optional Redis |
| Dash callbacks | Dash app | Remove from target | Replace with React client-side interaction and API contracts |

## Data Processing Requirements

### Raw And Bronze Processing

The pipeline must:

1. Discover source files from ADLS `raw_data/`.
2. Generate deterministic file identifiers.
3. Parse Excel files using a robust engine such as Polars/calamine.
4. Preserve file metadata: path, raw file name, size, last modified timestamp, ingestion timestamp.
5. Preserve worksheet/group information.
6. Extract raw channel identifiers, display names, units, and column index.
7. Convert raw wide worksheet data into long-format measurement facts.
8. Store conversion statistics per file and worksheet.
9. Track conversion success/failure and retry status.
10. Quarantine or flag corrupt/unparseable files.

Recommended bronze model:

| Table | Description |
| --- | --- |
| `bronze_filemeta` | Source file metadata |
| `bronze_channel` | Raw channel metadata from headers |
| `bronze_timeseries` | Generic long-format raw measurement facts |
| `bronze_statistics` | Conversion statistics |
| `raw_file_tracking` | Converter run status and retry metadata |

### Semantic Mapping Processing

The pipeline must map raw channels into canonical metrics using versioned mapping metadata.

Inputs:

- Raw channel identifier.
- Raw channel display name.
- Source file path.
- Worksheet/group.
- Series metadata.
- Mapping version.
- Canonical metric catalog.

Outputs:

- `series_id`.
- `file_id`.
- `worksheet_id` or `group_name`.
- `metric_id`.
- `metric_name`.
- `metric_unit`.
- `metric_origin`.
- `value_num`.
- `value_str`.
- `mapping_version_id`.
- `mapping_status`.

Required mapping behavior:

1. Use explicitly saved mappings first.
2. Support alias/channel-code matching from the canonical metric catalog.
3. Preserve unmapped raw channels for traceability.
4. Version mappings so historical processed data can be explained.
5. Store mapping version used by each pipeline run.
6. Produce mapping coverage metrics for UI status and DQ.

### Time Normalization Processing

The pipeline must:

1. Detect or map event timestamp columns.
2. Detect or map elapsed-time columns.
3. Convert `Date` + `Time` into `event_ts` where source files provide separate columns.
4. Normalize elapsed time into seconds.
5. Preserve raw timestamp strings when parsing fails.
6. Add `is_valid_timestamp` and timestamp parse status.
7. Support worksheet/file time stitching where elapsed time restarts.
8. Preserve original sample offset for lineage.

### Cleansing And Numeric Normalization

The pipeline must:

1. Cast canonical numeric metrics to numeric type.
2. Preserve non-numeric raw values in string fields where needed.
3. Convert invalid numeric values to null rather than failing the whole file.
4. Preserve enough lineage to trace bad values to file, worksheet, sample offset, and metric.
5. Produce DQ summaries for invalid/null/unmapped values.

### Derived KPI Processing

The following original app calculations must move into Databricks Silver.

| Derived metric | Formula / behavior | Required inputs |
| --- | --- | --- |
| `Energy Efficiency` | `1.48 * Faradaic Efficiency of CO / (Stack Voltage / 5)` | `Faradaic Efficiency of CO`, `Stack Voltage` |
| `Delta-p Anolyte` | `Anolyte inlet pressure - Anolyte outlet pressure` | `Anolyte inlet pressure`, `Anolyte outlet pressure` |
| `Current density` | `1000 * Current / active_area_cm2` | `Current`, stack active area |
| `Single Pass Conversion Efficiency` | `100 * ((Current * FECO/100) / (2 * F)) / ((CO2 flow / 60 / 5) / Vm)` | `Faradaic Efficiency of CO`, `Current`, `Cathode inlet CO2 gas flow` |

Constants from original app:

| Constant | Value | Usage |
| --- | ---: | --- |
| Faraday constant `F` | `96485.3` | SPCE calculation |
| Molar gas volume `Vm` | `22.414` | SPCE calculation |
| Default active area | `88.0 cm2` | Current density fallback |

Processing requirements:

1. If required inputs are missing, derived metric should be null and flagged, not silently calculated incorrectly.
2. Infinite values must become null.
3. Derived metrics must be distinguishable from source metrics using `metric_origin = calculation` or equivalent.
4. Active area must come from stack definition metadata, not hardcoded except as fallback.
5. Formula versions should be documented and testable.

### Plausibility And DQ Processing

Move plausibility behavior from the app into DQ/Silver logic.

Requirements:

1. Metric catalog must define expected unit and plausibility range.
2. Percent metrics should be constrained or flagged against `[0, 100]`.
3. Negative-only-invalid metrics should follow catalog rules such as `0,-` from the old schema.
4. Timestamp parseability must be checked.
5. Duplicate measurement keys must be checked.
6. Required metric coverage should be checked per series/report where applicable.
7. DQ output should support both engineering diagnostics and user-facing status.

Recommended DQ outputs:

| Table | Description |
| --- | --- |
| `silver_dim_dq_rule` | Rule catalog |
| `silver_fact_dq_result` | Row/metric-level failures |
| `silver_fact_dq_summary` | Summary by series/file/rule |
| `gold_dq_status` | App-facing status summary |

### Aggregation Processing

The original app creates raw, 1-minute, and 15-minute resolution behavior for plotting. This should move to Databricks Gold.

Required aggregations:

- Raw dashboard-ready data.
- 1-minute time-bin aggregations.
- 15-minute time-bin aggregations.

Aggregation behavior:

1. Use elapsed time in seconds as the primary binning axis.
2. For each metric and time bucket, calculate `min`, `max`, `mean`, and `count`.
3. Preserve bucket start time.
4. Exclude structural columns such as date/time/timestamp from numeric metric aggregation.
5. Support min/max envelope plotting.
6. Support dynamic resolution switching in the frontend/API.

Recommended aggregation shape:

| Column | Description |
| --- | --- |
| `series_id` | Series identifier |
| `metric_id` | Canonical metric identifier |
| `metric_name` | Denormalized display name |
| `resolution` | `raw`, `1min`, `15min` |
| `bucket_start_elapsed_s` | Bucket start in elapsed seconds |
| `event_ts_bucket_start` | Optional event timestamp bucket |
| `value_min` | Minimum value |
| `value_max` | Maximum value |
| `value_mean` | Mean value |
| `value_count` | Number of points |
| `quality_status` | Aggregated quality status |

## Data Model Format Decisions

### Canonical Format

Use long format as the canonical analytical model.

Canonical long measurement shape:

| Column | Description |
| --- | --- |
| `series_id` | Series identifier |
| `file_id` | Source file identifier |
| `group_name` | Worksheet/group |
| `sample_offset` | Source row/sample index |
| `event_ts` | Parsed event timestamp |
| `elapsed_time_s` | Elapsed time in seconds |
| `metric_id` | Canonical metric identifier |
| `metric_name` | Canonical metric name |
| `unit` | Canonical unit |
| `value_num` | Numeric value |
| `value_str` | Non-numeric value if applicable |
| `metric_origin` | `file` or `calculation` |
| `mapping_version_id` | Mapping version used |
| `quality_status` | Valid/warning/error status |

Reasoning:

- New metrics do not require schema changes.
- Custom reports can select arbitrary metric combinations.
- Metric catalog, mappings, tags, and DQ attach cleanly.
- Multiple stack/file formats can coexist.
- Columnar storage still performs well when filtered by metric/time/series.

### Wide Format

Use wide format only as a convenience/report-serving output, not as the source of truth.

Wide dashboard view example:

| Column | Description |
| --- | --- |
| `series_id` | Series identifier |
| `elapsed_time_s` | Elapsed time |
| `event_ts` | Event timestamp |
| `Stack Voltage` | Metric column |
| `Current` | Metric column |
| `Current density` | Derived metric column |
| `Energy Efficiency` | Derived metric column |
| `Single Pass Conversion Efficiency` | Derived metric column |

Use wide outputs for:

- Compatibility with existing standard report logic.
- Fast fixed report views.
- Formula validation tests.
- Simple CSV export for human users.

## Application UI/UX Scope

The new frontend should be a reporting and interaction layer. It should not calculate core metrics or mutate analytical facts.

### Main Navigation

Recommended app sections:

1. Standard Reports.
2. Custom Reports.
3. Series Catalog.
4. Pipeline Runs / Data Status.
5. Mapping Review.
6. Tag Configuration.
7. Saved Views.
8. Admin / Settings.

Only reporting features are part of the core app experience. Configuration screens support metadata entry and review; they do not process data locally.

### Standard Reports

Preserve original Standard Reports behavior.

Required features:

1. Select series.
2. Select predefined report.
3. Display primary and secondary y-axis metrics.
4. Support time slicer in hours.
5. Support range slider behavior.
6. Support manual Y1/Y2 range overrides.
7. Support dynamic resolution: raw, 1-minute, 15-minute.
8. Render min/max/mean bands for aggregated data.
9. Display active resolution and data quality status.
10. Save/export graph as HTML or image if required.

Predefined reports to preserve:

| Report | Y1 metrics | Y2 metrics |
| --- | --- | --- |
| All | all/selectable | optional |
| Voltage | `Stack Voltage` | `Cell Voltage - 1..5` |
| Gas Pressure | `Gas inlet pressure`, `Gas outlet pressure`, `Delta-p Gas inlet - Gas outlet` | none |
| CO2 Flow | `Cathode inlet CO2 gas flow` | none |
| Anolyte Pressure | `Anolyte inlet pressure`, `Anolyte outlet pressure`, `Delta-p Anolyte` | none |
| Anode Temperature | `Anode inlet`, `Anode outlet` | none |
| CO to H2 Ratio & Current Density | `CO:H2 ratio in cathode product gas` | `Current density` |
| Faradaic Efficiency | `Faradaic Efficiency of CO`, `Faradaic Efficiency of H2`, `Faradaic Efficiency of O2` | none |
| Energy Efficiency | `Energy Efficiency` | none |
| Single Pass Conversion Efficiency | `Single Pass Conversion Efficiency` | none |

### Custom Reports

Preserve original Custom Reports behavior.

Required features:

1. Select one or multiple series.
2. Select X-axis metric or elapsed time.
3. Select multiple Y1 metrics.
4. Select multiple Y2 metrics.
5. Support time-series mode when X is elapsed time.
6. Support scatter mode when X is a non-time metric.
7. Support time slicer.
8. Support X range slicer for non-time X-axis.
9. Support Y1/Y2 range overrides.
10. Support tag filters.
11. Support facet by tag.
12. Support legend grouping by up to a configured maximum number of tags.
13. Support data download for selected filtered data.
14. Support graph export.

### Series Catalog And Data Status

This replaces local Series Management as a workflow/status UI, not as a data processing engine.

Required features:

1. View series list.
2. View series metadata.
3. View associated files and worksheets.
4. View mapping version and mapping coverage.
5. View latest pipeline run status.
6. View data freshness.
7. View DQ status summary.
8. Trigger or request reprocessing if user is authorized.
9. Show warnings when data is not ready for reporting.

### Mapping Review UI

The app may preserve a mapping editor, but it should only edit metadata.

Required features:

1. View raw detected channels.
2. View canonical metric catalog.
3. Create/edit mapping entries.
4. Save mapping as a new version.
5. Compare mapping versions.
6. Submit mapping version for processing.
7. Show affected files/series.
8. Show mapping coverage and unmapped channels.

The mapping UI must not directly rewrite gold facts. It should save metadata and trigger processing.

### Tags And Facets UI

Preserve tag-based reporting behavior.

Required features:

1. Manage tag definitions.
2. Define source metric and unit divisor.
3. Define ordered categories.
4. Define per-series category ranges.
5. Apply tag filters in Custom Reports.
6. Use tags for facet and legend grouping.

Tag classification may be materialized in gold or calculated by the API for small slices. For consistency and performance, materialized gold tagged views are preferred when tags are stable.

### Export UX

Required export features:

1. Export graph as interactive HTML when feasible.
2. Export graph as PNG/JPEG/PDF if supported by the selected chart library.
3. Export filtered data as CSV.
4. Include elapsed time and test time in exports.
5. Include selected metric columns only.
6. Include metadata in export file name or export header.
7. Respect current filters, time range, series, and selected resolution.

## Frontend Interaction Model

Dash should not be the long-term target frontend because its callback-heavy client-server model is not ideal for many users performing frequent interactive chart operations.

Target interaction principle:

- Backend prepares data slices.
- Browser handles lightweight interaction.
- Browser does not receive the entire raw dataset.

Examples of browser-side interaction:

| Interaction | Browser-side? | API call needed? |
| --- | --- | --- |
| Hover tooltip | Yes | No |
| Legend toggle | Yes | No |
| Pan/zoom within loaded range | Yes | No |
| Y-axis range override | Yes | No |
| Toggle min/max band visibility | Yes | No |
| Change report | No | Yes |
| Change metric set | No | Yes |
| Change series | No | Yes |
| Expand time range beyond loaded window | Partial | Yes |
| Change tag filters | Usually no | Yes |
| Export full filtered data | No | Yes |

The API should return a bounded, chart-ready data slice according to a point budget. The frontend can then provide smooth interaction over that slice.

## API Data Delivery Strategy

The API must support different response formats depending on payload size and usage.

### Response Format Tiers

| Tier | Format | Use case |
| --- | --- | --- |
| Tier 1 | JSON | Small metadata, dropdowns, series list, report definitions, small chart slices |
| Tier 2 | Apache Arrow IPC / Feather | Medium-large chart data where browser/client can decode columnar payload |
| Tier 3 | Parquet export | Large export/download jobs, not usually direct interactive chart payload |
| Tier 4 | Arrow Flight SQL | Advanced high-throughput analytical transport if standard HTTP/API becomes bottleneck |

### JSON Endpoints

Use JSON for:

- Series metadata.
- Metric catalog.
- Report definitions.
- Tag definitions.
- Pipeline status.
- Small chart previews.
- UI configuration.

JSON is easy to debug and sufficient for low-volume responses.

### Arrow IPC Endpoints

Consider Arrow IPC for chart data responses when JSON becomes too large.

Benefits:

- Columnar transport.
- Smaller payloads than JSON for numeric arrays.
- Faster client-side parsing for large numeric data.
- Natural fit for long-format analytical results.

Possible endpoint shape:

```text
POST /api/charts/query
Accept: application/vnd.apache.arrow.stream
```

The browser can decode Arrow using Apache Arrow JavaScript.

### Arrow Flight SQL

Arrow Flight SQL is an advanced option, but should not be the first implementation.

Potential benefits:

- High-throughput columnar data transport.
- Efficient streaming of large analytical result sets.
- Good fit for service-to-service analytical data delivery.
- Avoids JSON serialization overhead.

Constraints:

- Adds protocol and infrastructure complexity.
- Browser support is not as straightforward as normal HTTP JSON/Arrow IPC.
- Authentication, authorization, gatewaying, and deployment are more involved.
- Databricks SQL connectivity should be validated against current platform support and operational constraints.
- Most UI interactions should not require direct database-style connectivity from the browser.

Recommended use:

- Use Arrow Flight SQL only behind the API or for backend service-to-service data movement if benchmarks prove HTTP JSON/Arrow IPC is insufficient.
- Do not expose Arrow Flight SQL directly to browsers as the primary app integration pattern.

Practical recommendation:

1. Start with FastAPI JSON for metadata and small chart payloads.
2. Add Arrow IPC over HTTP for large chart payloads.
3. Add asynchronous export endpoints for very large downloads.
4. Benchmark Databricks SQL latency and payload sizes.
5. Consider ClickHouse or Arrow Flight SQL only if the API data path becomes a proven bottleneck.

### API Point Budget

Every chart query should enforce a point budget to protect the browser and API.

Example policy:

| Query result size | API behavior |
| --- | --- |
| `< 20k points` | Return raw chart arrays |
| `20k-250k points` | Return Arrow IPC or downsampled/aggregated result |
| `> 250k points` | Force coarser resolution or require export job |
| Very large export | Asynchronous job, downloadable file |

Exact thresholds should be benchmarked with real data and selected chart library.

## Recommended API Endpoints

### Metadata

```text
GET /api/series
GET /api/series/{series_id}
GET /api/series/{series_id}/files
GET /api/metrics
GET /api/reports/standard
GET /api/tags/definitions
GET /api/pipeline/runs
GET /api/pipeline/runs/{run_id}
```

### Reporting

```text
POST /api/reports/standard/query
POST /api/reports/custom/query
POST /api/reports/custom/preview
POST /api/reports/export-data
POST /api/reports/export-graph
```

### Mapping And Configuration

```text
GET /api/series/{series_id}/mapping
POST /api/series/{series_id}/mapping-versions
GET /api/series/{series_id}/mapping-coverage
POST /api/series/{series_id}/request-reprocess
```

### Status And Quality

```text
GET /api/series/{series_id}/status
GET /api/series/{series_id}/dq-summary
GET /api/files/{file_id}/processing-status
```

## Chart Query Contract

Example request:

```json
{
  "series_ids": ["PoCII"],
  "report_key": "Voltage",
  "x_axis": "elapsed_time_s",
  "y1_metrics": ["Stack Voltage"],
  "y2_metrics": ["Cell Voltage - 1", "Cell Voltage - 2"],
  "time_range": {
    "min_h": 0,
    "max_h": 10
  },
  "resolution": "auto",
  "tag_filters": [],
  "max_points": 50000,
  "include_quality_flags": true
}
```

Example response shape for JSON:

```json
{
  "series": [
    {
      "series_id": "PoCII",
      "resolution": "raw",
      "x_unit": "h",
      "traces": [
        {
          "metric_name": "Stack Voltage",
          "axis": "y1",
          "unit": "V",
          "x": [0.0, 0.00027],
          "y": [12.1, 12.2],
          "quality": ["valid", "valid"]
        }
      ]
    }
  ],
  "warnings": [],
  "data_quality_summary": {
    "status": "valid"
  }
}
```

For aggregated data, traces should support min/max/mean:

```json
{
  "metric_name": "Stack Voltage",
  "axis": "y1",
  "unit": "V",
  "x": [0, 60, 120],
  "mean": [12.1, 12.2, 12.3],
  "min": [11.9, 12.0, 12.1],
  "max": [12.4, 12.5, 12.6]
}
```

## Backend Development Priorities

1. Define Postgres metadata schema.
2. Import existing local config into Postgres.
3. Implement API metadata endpoints.
4. Implement Databricks gold query service.
5. Implement standard report query endpoint.
6. Implement custom report query endpoint.
7. Add chart query point-budget logic.
8. Add Arrow IPC response option if JSON payload becomes too large.
9. Add export endpoints.
10. Add pipeline status and DQ status endpoints.

## Frontend Development Priorities

1. Build application shell and authentication.
2. Build Standard Reports page.
3. Build chart component with browser-side zoom, hover, legend, axis controls.
4. Build time slicer and resolution selector.
5. Build Custom Reports page.
6. Build metric selector and axis assignment UI.
7. Build tag filter/facet/legend UI.
8. Build export workflow.
9. Build Series Catalog and status pages.
10. Build Mapping Review page.

## Explicit Non-Goals For The Frontend

The frontend must not:

- Parse Excel files.
- Apply column mappings to raw data.
- Calculate scientific KPIs as source of truth.
- Perform plausibility clipping as source of truth.
- Generate official aggregates from raw data.
- Store canonical analytical facts locally.
- Replace Databricks DQ logic.

Frontend calculations are allowed only for display-level transformations such as unit display, hover formatting, chart scaling, local legend toggling, and temporary interaction over already-loaded data.

## Recommended First Delivery Slice

The first backend/frontend slice should prove the final architecture without adding unnecessary infrastructure.

Scope:

1. Postgres metadata for series, metrics, standard reports, mappings, and tags.
2. Databricks gold table or mock table for one migrated series.
3. FastAPI standard report query endpoint.
4. React Standard Reports page.
5. Plot one preserved report, such as Voltage.
6. Support time slicer and automatic raw/aggregation selection.
7. Return JSON first.
8. Add Arrow IPC only after measuring payload and latency.

Success criteria:

- User can select a series and standard report.
- App queries prepared gold data, not raw files.
- Browser handles zoom/hover/legend without server callbacks.
- Backend enforces point budget and resolution selection.
- Result matches local app report behavior for the selected sample dataset.
