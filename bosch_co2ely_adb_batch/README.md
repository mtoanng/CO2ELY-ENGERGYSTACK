# CO2ELY Databricks Pipeline

Production Databricks batch pipeline for CO₂ electrolysis analytics.  

## Overview

## Architecture

<img width="1085" height="616" alt="image" src="https://github.com/user-attachments/assets/97d7ee5c-0ab7-48cf-ab0b-32b959715e52" />



```
UC Volume (.xlsx uploads)
    │
    ▼ _1_r2b (Polars calamine)
┌─────────────────────┐
│  Bronze Delta Table │  bronze_co2_timeseries
└─────────────────────┘
    │
    ▼ _2_b2s (Polars)
┌─────────────────────┐
│  Silver Enriched    │  silver_co2_timeseries_enriched (12 metrics)
│  Silver Aggregated  │  silver_co2_timeseries_aggregated (15-min bins)
└─────────────────────┘
    │
    ▼ _3_s2g
┌─────────────────────┐
│  Gold Summary       │  gold_co2_summary_statistics (KPIs)
│  Gold Dashboard     │  gold_co2_timeseries_dashboard (chart data)
└─────────────────────┘
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
