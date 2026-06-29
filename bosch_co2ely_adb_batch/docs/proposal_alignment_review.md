# Proposal Alignment Review

## Scope

This review compares three things:

1. the current local CO_energystacck application behavior
2. the current Databricks pipeline implementation in bosch_co2ely_adb_batch
3. the target-state proposal in co_energystack_data_model_proposal.md

Status labels used throughout:

- Implemented: already present in the Databricks codebase as a real executable behavior
- Partial: present in some form, but incomplete, differently shaped, or not yet governed
- Missing: not materially implemented in the Databricks pipeline yet

---

## 1. Proposal Review By Section

### 1.1 Current Databricks Pipeline Shape

| Proposal statement | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| Databricks raw converter orchestrator exists | Implemented | src/_0_convert/run_converter.py | Real driver for source discovery, conversion, upload, and tracking |
| Excel parser/converter exists | Implemented | src/_0_convert/xlsx_converter.py | Real worksheet parser using Polars/calamine |
| Converter utility layer exists | Implemented | src/_0_convert/converter_utils.py | Real helper module for UUIDs, Parquet writing, lineage, tracking |
| Bronze ingestion exists | Implemented | src/_1_r2b/ingest_parquet_to_bronze.py | Real Auto Loader ingestion from raw Parquet to Delta |
| Raw Parquet staging layer already exists | Implemented | converter output contract plus parquet_raw paths | Correctly reflected in proposal |
| Bronze Delta layer already exists | Implemented | bronze_filemeta, bronze_channel, bronze_timeseries, bronze_statistics | Correctly reflected in proposal |

Assessment:
The proposal is accurate here. The current codebase is already an operational Databricks raw-to-bronze pipeline, not a local-only prototype.

### 1.2 Current Raw Output Contract

| Current entity in proposal | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| filemeta | Implemented | converter_utils.py schemas and builders | Present as current raw output and bronze input |
| channel | Implemented | converter_utils.py schemas and builders | Present and used downstream |
| timeseries | Implemented | converter_utils.py unpivot + bronze ingest | Long-format fact already exists |
| statistics | Implemented | converter_utils.py build_statistics | Present as per-group summary |

Assessment:
This is one of the strongest alignment points. The current raw contract is already materially closer to a normalized bronze design than the old local app.

### 1.3 Current Bronze Contract

| Proposal statement | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| Reads from parquet_raw/{table_type}/ | Implemented | src/_1_r2b/ingest_parquet_to_bronze.py | Correct |
| Writes bronze_filemeta | Implemented | build_table_name + TABLE_TYPES | Correct |
| Writes bronze_channel | Implemented | build_table_name + TABLE_TYPES | Correct |
| Writes bronze_timeseries | Implemented | build_table_name + TABLE_TYPES | Correct |
| Writes bronze_statistics | Implemented | build_table_name + TABLE_TYPES | Correct |

Assessment:
Accurate. Bronze ingestion is real and incremental.

### 1.4 Alignment With Long-Term Target

| Alignment area in proposal | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| Databricks is execution plane | Implemented | job resources + Python tasks | Correct |
| Delta used for bronze persistence | Implemented | ingest_parquet_to_bronze.py | Correct |
| Deterministic UUID lineage | Implemented | generate_file_uuid() | Correct |
| Long-format timeseries | Implemented | unpivot_timeseries() + bronze_timeseries | Correct |
| Channel metadata separated from measurements | Implemented | channel vs timeseries entity split | Correct |
| Bronze ingestion incremental/restartable | Implemented | Auto Loader + checkpoints | Correct |
| Worksheet lineage modeled durably | Missing | no bronze_worksheet durable entity | Proposal correctly calls this partial gap |
| Raw parse evidence preserved fully | Missing | no row-level issue table or full raw-value forensic model in bronze | Proposal correctly identifies this gap |
| DQ issues materialized explicitly | Partial | DQ tables exist in silver, not raw/bronze | Proposal should distinguish bronze gap vs silver progress |
| Metadata governed/versioned | Missing | still mainly code/config driven | Correct |
| Silver/gold serving semantics implemented | Partial | some silver/gold jobs exist, but not the full target model | Proposal is directionally right |

Assessment:
Mostly accurate, but the proposal should state more explicitly that DQ materialization has started in silver, not bronze.

### 1.5 Current Gaps Relative To Target Model

#### 1.5.1 Bronze Is Structured But Not Yet Forensic Enough

| Proposed gap | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| no dedicated bronze_worksheet entity | Missing | current model has filemeta/channel/timeseries/statistics only | Accurate |
| no durable bronze_ingest_issue table | Missing | no raw/bronze issue table | Accurate |
| no explicit source_row_number in bronze facts | Missing | sample_offset exists, but not original row lineage | Accurate |
| no always-preserved raw string value alongside parsed numeric value | Partial | bronze timeseries currently has value and value_str | Better than proposal implies, but still not full forensic preservation |
| no explicit parse-status per measurement row | Missing | no row-level parse status classification | Accurate |

Assessment:
This section is well aligned, except that current bronze already partially preserves raw string evidence via value_str.

#### 1.5.2 Converter Semantics Are Embedded In Code

| Proposed gap | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| row 1 / row 2 / row 3 semantics hardcoded | Implemented as current behavior | xlsx_converter.py | Accurate diagnosis |
| unit-row inference heuristic | Implemented as current behavior | detect_units_row() | Accurate diagnosis |
| channel deduplication logic embedded in converter | Implemented as current behavior | xlsx_converter.py unique channel handling | Accurate diagnosis |
| bronze preserves less raw evidence than ideal | Partial | converter still normalizes before bronze | Accurate |

Assessment:
Strong alignment. This is one of the key architectural truths the migration plan must respect.

#### 1.5.3 Metadata Is Not Yet Governed

| Proposed metadata governance gap | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| series metadata tables absent | Missing | no transactional metadata store in Databricks pipeline | Accurate |
| stack definitions governed table absent | Missing | still JSON/config driven in local app source material | Accurate |
| mapping versions absent | Missing | no governed mapping model yet | Accurate |
| formula registry absent | Missing | formulas still coded in local backend logic | Accurate |
| tag/report metadata tables absent | Missing | not implemented in Databricks path | Accurate |

Assessment:
Accurate and important.

#### 1.5.4 Operational Tracking And Analytical Bronze Are Still Blended Conceptually

| Proposal statement | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| tracking table exists and is useful | Implemented | IncrementalTracker in converter_utils.py | Accurate |
| tracking table is orchestration state, not analytical bronze | Correct architectural assessment | converter tracking table separate from bronze facts | Accurate |
| target architecture should separate run state from analytical facts | Missing as target behavior | no dedicated operational control-plane store yet | Accurate |

Assessment:
Strong alignment.

### 1.6 Refactored Bronze Model

| Target bronze entity | Status vs current pipeline | Evidence | Assessment |
| --- | --- | --- | --- |
| bronze_file | Partial | bronze_filemeta is a close precursor | Rename/contract hardening needed |
| bronze_worksheet | Missing | no durable worksheet table | Major gap |
| bronze_channel | Partial | bronze_channel exists but without full forensic semantics | Strong base exists |
| bronze_timeseries_long | Partial | bronze_timeseries exists, but missing row lineage and richer raw evidence | Strong base exists |
| bronze_ingest_issue | Missing | no issue table in bronze | Major gap |
| bronze_statistics | Partial | current bronze_statistics exists but is more operational summary than profiled bronze contract | Usable precursor |

Assessment:
The proposal is aligned if treated as an evolution from existing bronze tables, not a fresh schema invented from scratch.

### 1.7 Refactored Silver Model

| Target silver entity | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| silver_dim_metric | Missing | no governed metric dimension yet | Gap |
| silver_dim_series | Missing | no governed series dimension yet | Gap |
| silver_dim_mapping | Missing | no active mapping dimension yet | Gap |
| silver_fact_measurement_long | Partial | silver_fact_timeseries is the precursor | Existing silver base is a bridge, not final canonical model |
| silver_fact_derived_metric_long | Missing | no separate governed derived fact table | Gap |
| silver_measurement_quality | Partial | silver_fact_timeseries_dq_result and summary exist | DQ has begun, but not yet as a canonical measurement-quality model |
| silver_timeseries_wide_view | Missing | not implemented | Optional later serving compatibility layer |

Assessment:
Silver is the least mature layer relative to the proposal. There is meaningful progress, but the governed canonical model is not implemented yet.

### 1.8 Refactored Gold Model

| Target gold entity | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| gold_timeseries_raw | Missing | no metric-grain raw serving fact in gold | Gap |
| gold_timeseries_1min | Missing | no durable aggregated gold timeseries yet | Gap |
| gold_timeseries_15min | Missing | no durable aggregated gold timeseries yet | Gap |
| gold_metric_summary | Partial | gold_summary_statistics.py exists | Present, but simpler than target |
| gold_report_metric_catalog | Missing | no report metadata serving layer | Gap |
| gold_tagged_timeseries | Missing | no tag-enriched Delta view/table | Gap |
| gold_dq_status | Missing | no gold-facing DQ summary model | Gap |
| dashboard-ready projection | Partial | gold_timeseries_view.py exists | Present, but narrow and not yet aligned with long-format serving strategy |

Assessment:
Gold is only partially aligned. What exists today is more of a dashboard compatibility layer than a mature gold model.

### 1.9 Refactored Metadata Model

| Metadata domain | Status | Evidence | Assessment |
| --- | --- | --- | --- |
| transactional metadata store | Missing | none in current Databricks codebase | Gap |
| versioned mappings | Missing | no DB-backed metadata plane | Gap |
| versioned formulas | Missing | local backend formulas only | Gap |
| pipeline_run state model | Missing | no dedicated governed run-state plane | Gap |
| saved frontend state | Missing | outside current Databricks scope | Gap |

Assessment:
Accurate target, not implemented.

### 1.10 Migration Strategy Alignment

| Proposed migration phase | Status | Assessment |
| --- | --- | --- |
| Phase 1 harden current raw and bronze | Strongly aligned | This is the right next move and fits the real codebase |
| Phase 2 introduce governed silver | Aligned but hard | This is the major design and implementation step |
| Phase 3 build gold around current UI semantics | Aligned | Should happen only after silver parity is proven |
| Phase 4 retire local CSV serving logic | Aligned | Correct end state |

Assessment:
The migration sequence is strategically correct, but it needs stricter deliverables and acceptance gates.

---

## 2. Gap Matrix: Local App Logic vs Databricks Silver/Gold Responsibilities

| Local app responsibility | Current local implementation | Databricks target responsibility | Current Databricks status | Gap type | Required next move |
| --- | --- | --- | --- | --- | --- |
| Series registry and lifecycle | SeriesDataManager + series.json | Control-plane metadata store and silver dim_series | Missing | Control-plane gap | Define governed series model and ingestion path |
| File import to bronze | import_file_to_bronze() | Raw source file registry + operational tracking | Partial | Boundary mismatch | Separate source-file metadata from converter tracking state |
| Worksheet parsing directives | stored in per-series config | bronze_worksheet + worksheet_config metadata | Missing | Contract gap | Add worksheet entity and parsing lineage |
| Column mapping | mapping JSON per series | silver_dim_mapping + mapping_version | Missing | Governance gap | Externalize mappings and version them |
| Date + Time -> Timestamp | apply_mapping() | silver timestamp normalization | Partial | Responsibility blur | Move canonical timestamp semantics to silver contract |
| Elapsed-time stitching | load_and_concat_files_and_worksheets() | bronze lineage + silver canonical elapsed-time rules | Missing/Partial | Semantic gap | Preserve raw evidence in bronze, formalize continuity rules |
| Numeric coercion to NaN | clean_data() | silver canonical parsing plus quality flags | Missing | DQ gap | Add explicit parse-status and raw-vs-clean value model |
| Energy Efficiency KPI | data_enrichment.py | silver derived metric logic with versioned formula | Missing | Formula governance gap | Port formula with parity tests |
| Delta p Anolyte KPI | data_enrichment.py | silver derived metric logic with versioned formula | Missing | Formula governance gap | Port formula with parity tests |
| Current density KPI | data_enrichment.py | silver derived metric logic with stack metadata | Missing | Formula + metadata gap | Govern stack metadata and port formula |
| SPCE KPI | data_enrichment.py | silver derived metric logic with versioned formula | Missing | Formula governance gap | Port formula with parity tests |
| Plausibility clipping | apply_plausibility_limits() | silver DQ rules and rule-action model | Partial | Rule model gap | Add generic plausibility-rule engine |
| Aggregation min/max/mean | aggregate_timeseries() | gold_timeseries_1min / 15min / summaries | Missing | Serving-model gap | Build durable aggregated gold tables |
| Tag classification | tag_manager/in-memory logic | metadata-driven tag model + optional gold tagged view | Missing | Serving + metadata gap | Model tags in metadata and decide materialization strategy |
| Standard reports | JSON config | transactional metadata + gold report catalog | Missing | Metadata gap | Govern report definitions |
| Dynamic resolution selection | frontend runtime behavior | API/gold serving strategy | Missing | Serving gap | Publish raw/1min/15min gold contracts |
| DQ visibility | implicit NaN/clipping | silver_measurement_quality + gold_dq_status | Partial | DQ gap | Expand current DQ work beyond timeseries rule tables |

Key reading of the matrix:

- Raw/bronze already replace the local file-copy bronze model.
- Silver is where most local-app responsibilities need to be re-expressed industrially.
- Gold is currently too thin to replace the local reporting semantics.

---

## 3. Stricter Migration Blueprint

## Phase 1: Harden Current Raw And Bronze

### Objective

Turn the current converter/bronze pipeline into a durable bronze contract with explicit lineage and operational safety, without trying to solve governed business semantics yet.

### Deliverables

1. Bronze contract specification
- Define canonical schemas for bronze_file, bronze_channel, bronze_timeseries_long, bronze_statistics.
- Declare which current fields are stable, transitional, or deprecated.
- Add explicit run identifiers to all raw/bronze outputs.

2. Worksheet lineage model
- Introduce bronze_worksheet as a durable Delta entity.
- Every channel and timeseries row must join back to worksheet_id.

3. Row-level lineage additions
- Add source_row_number to bronze_timeseries_long.
- Preserve sample_offset as processing-friendly derived lineage, not the only row locator.

4. Raw evidence preservation
- Ensure bronze_timeseries_long always keeps raw_value_string and parsed_value_double semantics explicitly.
- Define parse outcome flags instead of relying on null inference alone.

5. Bronze ingest issue model
- Introduce bronze_ingest_issue.
- Minimum issue classes: unreadable worksheet, missing header rows, ambiguous elapsed-time offset, duplicate channels, parse failures.

6. Operational-control separation
- Keep converter tracking table for orchestration.
- Document explicitly that it is not analytical bronze.
- Add run_id or converter_run_id consistently to bronze outputs.

7. Integration-test isolation
- Preserve isolated raw/bronze test paths and test tables as hard deployment requirements.
- Verify that integration-test data paths do not overlap production bronze external locations.

8. Raw/bronze parity test set
- Add executable tests for file lineage, worksheet lineage, sample offset continuity, and issue-table writes.
- Add at least one fixture covering multi-sheet elapsed-time stitching.

### Exit Criteria

- bronze_file, bronze_worksheet, bronze_channel, bronze_timeseries_long, bronze_statistics, bronze_ingest_issue exist as explicit contracts.
- Every bronze_timeseries_long row is traceable to file_id, worksheet_id, source_row_number, and run_id.
- Integration-test raw/bronze runs are isolated from production data paths.
- Raw/bronze E2E path is executable on Databricks test infrastructure.

### Non-Goals

- Do not introduce Postgres metadata dependencies yet.
- Do not move KPI formulas into Phase 1.
- Do not attempt final dashboard-serving gold design yet.

## Phase 2: Introduce Governed Silver

### Objective

Convert bronze parse outputs into a governed canonical metric layer with versioned mapping, explicit DQ, and formula parity with the local application.

### Deliverables

1. Canonical metric model
- Define silver_dim_metric.
- Decide canonical metric identifiers and unit governance rules.
- Establish alias-handling and metric versioning rules.

2. Series and mapping governance bridge
- Define minimal series dimension needed for analytics.
- Introduce mapping_version and mapping_entry metadata model.
- Materialize silver_dim_mapping snapshot into Databricks for joins.

3. Canonical measurement fact
- Build silver_fact_measurement_long from bronze_timeseries_long joined through mapping/channel metadata.
- Required columns: series_id, file_id, worksheet_id, source_row_number, elapsed_time_s, event_timestamp, metric_id, value_double, raw_value_string, unit, origin, mapping_version_id.

4. Formula parity workstream
- Port local formulas exactly from data_enrichment.py.
- Add parity fixtures comparing Databricks outputs to local Pandas outputs for:
  - Energy Efficiency
  - Delta p Anolyte
  - Current density
  - Single Pass Conversion Efficiency
- Introduce formula_version for derived outputs.

5. Explicit DQ materialization
- Expand from current rule-result tables to a canonical silver_measurement_quality model.
- Distinguish parse failure, missing required input, divide-by-zero risk, plausibility clipping, invalid timestamp, duplicate point key.
- Preserve original value and rule outcome instead of only deleting/annotating rows.

6. Plausibility rule engine
- Generalize beyond percentage clipping.
- Support bounded numeric rules from governed metadata.
- Store rule_id, action, original_value, adjusted_value, and severity.

7. Timestamp and elapsed-time semantics
- Standardize event_timestamp and elapsed_time_s semantics in silver.
- Decide whether timestamp channels remain facts, become structural metadata, or both.

8. Silver compatibility views
- If downstream consumers still need easier access, publish non-canonical compatibility views rather than wide canonical tables.

### Exit Criteria

- silver_dim_metric, silver_dim_mapping, silver_fact_measurement_long, and silver_measurement_quality exist and are populated from bronze.
- KPI outputs match local app formulas on agreed fixture datasets.
- DQ outcomes are explicit and queryable, not implicit null side effects.
- Canonical silver facts can support future raw/1min/15min gold outputs without depending on local CSV logic.

### Non-Goals

- Do not fully replace the frontend/API yet.
- Do not attempt arbitrary interval gold materialization yet.
- Do not require the full transactional metadata platform if a staged metadata snapshot approach is sufficient.

---

## Recommended Immediate Next Steps

1. Freeze and document the current bronze schema contract before expanding it.
2. Design bronze_worksheet and bronze_ingest_issue first, because they unblock lineage and DQ in every later step.
3. Build formula-parity fixtures from the local app now, before silver logic drifts.
4. Treat metadata governance as staged: start with the minimum needed for silver mapping and formulas, not the entire target metadata universe at once.
5. Keep gold out of scope until canonical silver is trustworthy.

---

## Bottom Line

The proposal is strategically aligned with the Databricks pipeline if it is used as an evolutionary plan.

The current raw/bronze pipeline is already a strong industrial foundation.

The real migration difficulty is not replacing the converter; it is:

- formalizing forensic bronze lineage
- translating local Pandas enrichment into governed silver contracts
- making DQ explicit
- introducing governed metadata without stalling delivery
- building gold in a way that preserves current UI semantics without carrying forward local wide-file patterns
