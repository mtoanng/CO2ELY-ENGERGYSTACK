# CO2ELY Databricks Pipeline

Databricks batch pipeline for CO₂ electrolysis analytics, packaged as a Databricks Asset Bundle.

## Overview

The pipeline processes sheet-based XLSX measurements into curated analytical tables for downstream querying.

High-level flow:

```text
Raw XLSX files
-> converter parquet outputs
-> bronze Delta tables
-> silver dimensions
-> gold timeseries and 1-minute aggregates
-> serving aggregates and experiment index
-> SQL Warehouse / application queries
```

## Data Flow

### Converter
- Reads XLSX / XLS files from storage
- Resolves series-specific mapping metadata
- Normalizes structural time columns
- Computes derived metrics
- Applies plausibility clipping for percentage-based metrics
- Writes raw parquet datasets:
  - `filemeta`
  - `channel`
  - `timeseries`
  - `statistics`

### Bronze
- Ingests converter parquet outputs with Auto Loader
- Publishes Delta tables:
  - `bronze_filemeta`
  - `bronze_channel`
  - `bronze_timeseries`
  - `bronze_statistics`

### Silver
- Creates experiment identity from `uuid + group`
- Creates signal identity and canonical channel mapping
- Publishes Delta tables:
  - `silver_dim_experiment`
  - `silver_dim_signal`

### Gold
- Joins bronze timeseries with silver signal metadata
- Publishes:
  - `gold_timeseries`
  - `gold_timeseries_agg_1min`

### Serving
- Re-aggregates 1-minute gold data into coarser query surfaces
- Publishes:
  - `gold_timeseries_agg_15min`
  - `gold_timeseries_agg_60min`
  - `gold_channel_catalog_experiment`
  - `gold_experiment_index`

## Jobs

The end-to-end job runs the following stages sequentially:

```text
job_co2ely_converter
-> job_co2ely_bronze
-> job_co2ely_silver
-> job_co2ely_gold
-> job_co2ely_serving
```

The bundle also defines integration-test variants for the stage jobs.

## Quick Start

```bash
# Validate bundle
databricks bundle validate -t dev_user

# Deploy to personal workspace
databricks bundle deploy -t dev_user

# Run the full pipeline
databricks bundle run job_co2ely_e2e -t dev_user
```

## Environments

| Target | Purpose | Schedule |
| --- | --- | --- |
| dev_user | Personal development workspace | Manual |
| dev | Shared development workspace | Manual |
| qa | Quality assurance workspace | Manual |
| prod | Production workspace | Weekdays 18:00 Europe/Amsterdam |

## Dependencies

- `polars[calamine]>=1.0`
- `fastexcel`
- `azure-storage-blob>=12.19`
- `azure-identity>=1.15`
- PySpark runtime on Databricks clusters

## Reference

- End-to-end lineage spec: `docs/lineage_e2e_excalidraw_spec.md`
- Excalidraw lineage diagram: `docs/lineage_e2e.excalidraw`
