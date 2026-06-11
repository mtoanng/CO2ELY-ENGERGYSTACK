"""co_energystack_pipeline — Production Databricks pipeline for CO₂ electrolysis analytics.

Migrated from CO_energystacck Dash app (Azure App Service) to Databricks batch jobs.
Uses the same Polars (calamine) engine for Excel processing.

Medallion architecture:
    _1_ingest  : UC Volume (.xlsx) → Bronze Delta tables (Polars/calamine read)
    _2_enrich  : Bronze → Silver (Polars enrichment: EE, CD, FE, flows, SPCE)
    _3_gold    : Silver → Gold (dashboard-ready views + summary KPIs)
    _common    : Shared utilities (env detection, logging, Delta I/O)
"""
