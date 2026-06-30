# CO-ESTACK-ADB / co_energystack_pipeline

Production Databricks batch pipeline for CO₂ electrolysis analytics.  
**Migrated from** the CO_energystacck Dash app (Azure App Service) to Databricks Jobs.

Built as a **Declarative Automation Bundle** (DAB).

## Key Migration Decisions

| App (CO_energystacck) | Pipeline (CO-ESTACK-ADB) |
| --- | --- |
| Polars + calamine for xlsx | Same — no Spark Excel JAR needed |
| Local `/home/data/` + ADLS sync | UC Volumes → Delta tables |
| Single-user App Service | Multi-env: dev/qa/prod |
| In-app enrichment | Reusable polars_engine wheel |
| Dash frontend reads Parquet | Frontend reads from gold Delta/views |

## Architecture

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

## Repo Structure (for CO-ESTACK-ADB)

```
CO-ESTACK-ADB/                          ← Git repo root
└── co_energystack_pipeline/            ← Bundle root
    ├── databricks.yml
    ├── .gitignore
    ├── resources/
    │   ├── job_co2ely_bronze.yml
    │   ├── job_co2ely_silver.yml
    │   ├── job_co2ely_gold.yml
    │   └── job_co2ely_e2e.yml
    └── src/
        ├── __init__.py
        ├── _1_r2b/
        │   └── ingest_excel_to_bronze.py
        ├── _2_b2s/
        │   ├── enrich_timeseries.py
        │   └── aggregate_timeseries.py
        ├── _3_s2g/
        │   ├── gold_summary_statistics.py
        │   └── gold_timeseries_view.py
        └── _5_common/
            ├── common_utils.py
            └── common_io_utils.py
```

## Quick Start

```bash
# Validate bundle
databricks bundle validate -t dev_user

# Deploy to personal dev workspace
databricks bundle deploy -t dev_user

# Run end-to-end pipeline
databricks bundle run job_co2ely_e2e -t dev_user

# Deploy to production
databricks bundle deploy -t prod
```

## Environments

| Target | Workspace | Schedule |
| --- | --- | --- |
| dev_user | DEV (personal) | Manual |
| dev | DEV (shared, SP) | Manual |
| qa | QA (SP) | Manual |
| prod | PROD (SP) | 18:00 MON-FRI Amsterdam |

## Dependencies

- `polars[calamine]>=1.0` — Excel reads + enrichment engine
- PySpark (cluster runtime) — Delta I/O
- No external JARs required

## TODOs

- [ ] Package polars_engine as a wheel (share between app and pipeline)
- [ ] Add config CSV for dynamic source discovery (like bosch_ely_adb_batch bronze_config)
- [ ] Add DQM task (data quality monitoring)
- [ ] Add ADLS Parquet sync for Dash app reads
- [ ] Wire integration tests
