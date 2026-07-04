# CO2ELY Databricks Pipeline - Resume Project Summary

## Project Overview

This project is an end-to-end Azure Databricks batch data pipeline for CO2 electrolysis analytics. It converts raw Excel-based laboratory and experiment measurement files into curated analytical datasets that can be queried by downstream applications and SQL consumers.

The solution follows a medallion-style architecture and is packaged as a Databricks Asset Bundle for repeatable deployment across development, QA, and production environments.

At a high level, the pipeline performs:

- raw workbook ingestion from Azure storage
- Excel sheet parsing and structural normalization
- derived metric calculation
- Bronze Delta ingestion
- Silver conformed dimension building
- Gold timeseries and aggregate generation
- serving-layer table creation for application and analytics access

## Business / Domain Context

The pipeline processes CO2 electrolysis experiment data. These experiments produce workbook-based measurement outputs containing time series, channel metadata, and summary statistics. The engineering goal is to transform these semi-structured files into stable, queryable datasets that support analysis, reporting, and application-facing features.

This is not only a file ingestion pipeline. It is a domain-aware analytical data product that standardizes experimental signals, preserves experiment identity, and exposes curated aggregates suitable for downstream consumption.

## Main Technology Stack

### Core Platform

- Azure Databricks
- Databricks Jobs
- Databricks Asset Bundles
- Databricks Runtime 15.4.x (Spark runtime)
- Unity Catalog-style catalog and schema organization

### Compute / Processing

- PySpark for distributed ETL and Delta operations
- Polars for fast workbook parsing and local dataframe reshaping
- `calamine` engine for Excel reading
- `fastexcel` for efficient Excel support
- Python 3.11

### Storage / Table Layer

- Delta Lake tables
- Azure Data Lake Storage Gen2
- ABFSS paths for external table locations

### Engineering / Quality

- pytest
- mypy
- ruff
- environment-aware configuration and reusable utility modules

## End-to-End Architecture

The pipeline is organized into the following stages:

```text
Raw XLSX / XLS files
-> converter outputs
-> Bronze Delta tables
-> Silver dimensions
-> Gold fact and aggregate tables
-> serving tables
-> SQL Warehouse / downstream application queries
```

### Converter Layer

The converter reads Excel files and processes individual sheets. It performs the file-oriented and workbook-specific work before the data is pushed into Spark-native table processing.

Key responsibilities:

- read `.xlsx` and `.xls` files from storage
- parse multi-row headers
- normalize structural time columns
- detect and merge split date/time fields into canonical timestamps
- standardize channel and unit metadata
- compute derived metrics
- clip or constrain plausibility-sensitive percentage metrics where needed
- produce parquet-ready outputs for downstream Bronze ingestion

Produced outputs include:

- `filemeta`
- `channel`
- `timeseries`
- `statistics`

### Bronze Layer

The Bronze layer ingests the converter outputs into Delta tables. This stage establishes durable raw-to-structured persistence for downstream transformations.

Published Bronze tables:

- `bronze_filemeta`
- `bronze_channel`
- `bronze_timeseries`
- `bronze_statistics`

### Silver Layer

The Silver layer builds conformed dimensions used to stabilize experiment identity and signal identity.

Key responsibilities:

- generate experiment identity from sheet-level business keys such as `uuid + group`
- standardize signal metadata
- map raw channels to canonical channels
- create deterministic signal identifiers
- enforce uniqueness and dimensional consistency

Published Silver tables:

- `silver_dim_experiment`
- `silver_dim_signal`

### Gold Layer

The Gold layer joins fact-level timeseries with conformed signal metadata and generates analytics-ready datasets.

Key responsibilities:

- enrich raw timeseries with Silver dimension data
- build queryable Gold timeseries facts
- generate 1-minute aggregate tables for downstream reuse

Published Gold tables:

- `gold_timeseries`
- `gold_timeseries_agg_1min`

### Serving Layer

The serving layer produces application-oriented tables designed for faster downstream access.

Key responsibilities:

- re-aggregate Gold 1-minute data into coarser resolutions
- create experiment-level KPI and metadata views
- expose channel availability per experiment
- support SQL Warehouse or application-driven access patterns

Published serving tables:

- `gold_timeseries_agg_15min`
- `gold_timeseries_agg_60min`
- `gold_channel_catalog_experiment`
- `gold_experiment_index`

## Key Engineering Techniques And Concepts

### 1. Polars and Spark Integration

One of the strongest technical patterns in this project is the split between file-oriented processing and distributed analytical processing.

Polars is used where it performs best:

- Excel workbook parsing
- local dataframe reshaping
- header and schema normalization
- sheet-level preprocessing
- efficient file I/O workloads

Spark is used where distributed compute is the better tool:

- joins across larger datasets
- aggregations across experiment data
- dimensional modeling
- Delta Lake persistence
- serving-table generation

This is a good example of using the right engine for the right workload instead of forcing all logic into a single framework.

### 2. Medallion Architecture

The project follows a Bronze, Silver, Gold design:

- Bronze preserves structured raw data from ingestion
- Silver standardizes and conforms core business entities
- Gold creates analytics-ready facts and aggregates
- serving tables optimize data access for application and warehouse consumers

This demonstrates understanding of layered data modeling, separation of concerns, and maintainable pipeline design.

### 3. Deterministic Identity Modeling

The pipeline uses deterministic ID generation for experiment and signal entities. Rather than relying on transient runtime state, IDs are derived from stable business keys.

Examples:

- `experiment_id` derived from `uuid` and `group`
- `signal_id` derived from experiment-related keys and channel identity

This improves reproducibility, simplifies incremental loading, and reduces downstream ambiguity.

### 4. Sheet-Level Grain Awareness

The source files are workbook-based and contain multiple sheets. A key modeling requirement is that sheet-level identity matters.

The pipeline treats workbook sheets as meaningful experimental units by carrying identifiers like `group` through downstream layers. This avoids grain mismatch problems such as duplicate rows caused by joining sheet-grain metadata at file-grain only.

This is a strong example of practical dimensional modeling and grain management.

### 5. Canonical Signal Mapping

The pipeline maps raw measurement channels to standardized channel names. This enables consistent analytics across differently labeled files and experimental outputs.

This concept includes:

- raw channel to standardized channel mapping
- unit handling
- signal metadata harmonization
- downstream KPI calculation against canonical signals

### 6. Derived Metric Computation

The converter layer computes derived metrics before downstream ingestion. This shows that the pipeline is not only loading source data, but also applying domain logic and calculated features during transformation.

### 7. Weighted Aggregation Semantics

The serving layer uses metric-aware aggregation logic. For example, averages are computed using weighted means based on observation counts, while peak-style metrics use maximum semantics.

This is important because analytical correctness depends on metric-specific aggregation rules rather than naive averaging.

### 8. Incremental / Append-Oriented Processing

Several stages are designed to append only new entities or new experiment outputs rather than rewriting everything on every run.

Benefits:

- improved efficiency
- lower recomputation cost
- simpler production execution patterns
- easier operational scaling

### 9. Data Quality And Validation

The project includes dedicated validation and DQ-oriented logic. This demonstrates engineering maturity beyond just transformation logic.

Examples of quality concerns handled in the codebase include:

- uniqueness checks
- dimensional consistency checks
- schema alignment
- readiness validation for downstream tables

## Azure Databricks Features Demonstrated

This project showcases several important Azure Databricks capabilities.

### Databricks Jobs

The pipeline is split into stage-specific jobs for:

- converter
- Bronze
- Silver
- Gold
- serving
- end-to-end execution
- integration-test variants

This reflects real production orchestration instead of ad hoc notebook execution.

### Databricks Asset Bundles

The project is packaged and deployed using Databricks Asset Bundles. This is important because it shows infrastructure-as-code style deployment and environment promotion discipline.

Capabilities demonstrated:

- reusable YAML-based job definitions
- per-environment target configuration
- consistent deployment across workspaces
- parameterized runtime behavior

### Cluster Configuration And Autoscaling

The repo includes both:

- single-node cluster patterns for lightweight development or integration scenarios
- autoscaled job cluster patterns for production pipeline stages

This shows understanding of workload sizing, cost-awareness, and stage-appropriate cluster design.

### Delta Lake Optimization Features

The Spark jobs enable Delta-specific write optimizations such as:

- optimize write
- auto compact
- schema merge behavior where required

These are important platform-level practices for stable Delta writes and manageable storage layout.

### External Table Management On ADLS Gen2

The pipeline builds external Delta table locations using ABFSS paths, which demonstrates practical understanding of cloud object storage integration with Databricks and governed table management patterns.

### Multi-Environment Deployment

The bundle supports multiple targets such as:

- personal development workspace
- shared development workspace
- QA workspace
- production workspace
- integration-test deployments

This is valuable resume material because it shows that the solution was built with controlled promotion and environment separation in mind.

### Service Principal And Secret-Based Execution

The job configuration uses service principal and secret-backed patterns for non-interactive execution. This reflects enterprise deployment practices rather than local-only engineering.

## Operational And Data Engineering Strengths

This project demonstrates several strengths that are useful to highlight in a resume or interview:

- building structured pipelines from semi-structured business data
- combining local-style dataframe tools and distributed engines effectively
- designing medallion-layer data models
- handling workbook-specific ingestion complexity
- building conformed dimensions and serving-friendly facts
- packaging jobs for reproducible cloud deployment
- thinking about correctness, grain, and aggregation semantics
- validating data quality and operational stability

## Strong Resume Positioning

You can describe the project as:

A production-style Azure Databricks batch analytics pipeline for industrial experiment data that ingests Excel-based source files, standardizes measurement signals, builds Delta Lake medallion tables, and publishes query-optimized serving datasets for downstream applications and analytics.

## Resume Bullet Examples

### Option A - Technical / Balanced

- Built and maintained an Azure Databricks batch pipeline that transformed raw Excel-based electrolysis experiment files into curated Bronze, Silver, Gold, and serving Delta tables for downstream analytics and application access.
- Implemented a hybrid Polars and PySpark architecture, using Polars for efficient Excel/file I/O and schema normalization, and Spark for distributed joins, dimensional modeling, aggregations, and Delta Lake persistence.
- Packaged and deployed the platform with Databricks Asset Bundles across dev, QA, and prod environments, including parameterized jobs, shared cluster policies, and integration-test execution paths.
- Developed conformed experiment and signal dimensions with deterministic key generation and canonical channel mapping to improve data consistency, reproducibility, and downstream query reliability.

### Option B - Junior Data Engineer Friendly

- Worked on an Azure Databricks ETL pipeline that ingests laboratory Excel files and transforms them into clean analytical Delta tables using a medallion architecture.
- Used Polars for fast Excel parsing and Spark for scalable data transformation, aggregation, and table creation.
- Helped build Silver and Gold layer datasets, including experiment dimensions, signal mapping, and serving tables for application and SQL analytics use cases.
- Supported multi-environment deployment, validation, and pipeline quality checks using Databricks jobs, Asset Bundles, and Python testing tools.

### Option C - Stronger Product / Impact Framing

- Engineered a cloud-based analytical data pipeline on Azure Databricks for CO2 electrolysis experiments, converting semi-structured workbook outputs into governed, application-ready Delta Lake datasets.
- Improved downstream data usability by standardizing channel metadata, generating derived metrics, and publishing query-optimized experiment indexes and multi-resolution timeseries aggregates.
- Applied dimensional modeling, deterministic entity keys, and metric-aware aggregation logic to improve analytical correctness and reproducibility across batch pipeline runs.

## Interview Talking Points

If you need to explain the project in an interview, these are strong points to emphasize:

- why Polars was useful for Excel-heavy ingestion while Spark was better for downstream distributed processing
- how medallion architecture helped separate raw ingestion from conformed modeling and final serving
- why deterministic IDs and correct grain handling mattered for preventing duplicate or inconsistent outputs
- how Gold and serving tables were designed for consumer-facing query patterns rather than only internal ETL storage
- how Databricks Asset Bundles and environment-aware job definitions supported production deployment discipline

## One-Line Summary

Built and maintained an Azure Databricks medallion ETL pipeline that used Polars for efficient Excel ingestion and Spark/Delta Lake for scalable transformation, dimensional modeling, aggregation, and serving-layer analytics delivery.
