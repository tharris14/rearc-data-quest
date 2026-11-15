# PROCESS.md — Rearc Data Quest

## 1. Approach and architecture

I built this as a small medallion architecture in Databricks using Spark Declarative Pipelines (SDP). I separated external HTTP ingestion from Spark transformations so the source download concerns (access policy, retry logic, versioning) do not leak into the declarative model.

- **Source ingestion (Python):** Discover every direct file in the BLS `pr/` directory from its HTML listing rather than maintaining a list of filenames. The HTTP `User-Agent` supplies contact information as requested by BLS policy; the email comes from environment/private Volume configuration rather than the repository. Data USA is called with explicit cube, drilldown, locale and measure parameters. Retry 429 and transient 5xx responses with backoff; fail on persistent access errors instead of circumventing restrictions.
- **Landing storage (Unity Catalog Volume):** Download raw bytes; calculate SHA-256; store each unique version at `raw/<source>/<filename>/<hash>/<filename>`. Retain earlier versions for traceability and debugging. On successful completion of all downloads, publish a JSON snapshot of the current active inventory. Unchanged bytes reuse an existing path and do not create a new manifest.
- **Bronze (streaming tables):** Auto Loader/`STREAM read_files` ingests immutable file versions incrementally, preserving raw BLS text values and file paths. Bronze also captures source manifest history and the original population records as a nested array with safely normalized field names (`Nation ID` -> `nation_id`).
- **Silver (materialized views):** Select only the **latest active source manifest**, cast and validate numeric/required fields, filter to the US population, flatten JSON records, and resolve duplicate `series_id`/`year`/`period` observations. BLS `Current` wins over `AllData` if overlapping rows disagree; existing data showed no value conflicts. Data-quality expectations drop invalid keys, invalid periods/years and nonnumeric/invalid observations, while making violations observable in pipeline metrics.
- **Gold (materialized views):** Materialize three answers in **Spark SQL**, which is my primary analytical implementation because its grouped aggregations, joins and window-based ranking are concise and easy to audit. Implement each analysis a second time independently with the **PySpark DataFrame API**. A validation script compares the two outputs, including null-preserving joins.

The active-file manifest is essential: Auto Loader may preserve older file versions in Bronze, but Silver should **not** continue reporting them once the source is modified or removed. Materialized views are refreshed against the current manifest rather than relying on an append-only Silver stream.

## 2. Business rules and quality judgments

**Population:** Use the `United States` record (`nation_id = 01000US`) and years 2013 through 2018 inclusive. Return both `STDDEV_POP` and `STDDEV_SAMP`, since the prompt does not state which convention it expects. The former is the primary interpretation of the six listed annual observations. Retain missing population years as null in the Gold join; do not invent values. The observed API response had 11 years, with 2020 absent.

**Best year by series:** BLS periods `Q01`–`Q04` represent quarterly data; `Q05` represents annual data and must not be summed with quarters. The observed data contains 283 series with quarterly observations and 54 with annual observations only. For quarterly series, sum `Q01`–`Q04` per year; for annual-only series, select the highest `Q05` value (clearly labeled `annual_q05`). The two calculation methods are not directly comparable and the annual-only result is an explicit interpretation needed to return all 337 series. Years with fewer than four available quarters are eligible, matching the request to sum available quarters. On ties, choose the earliest year. Join `pr.series` to expose a human-readable `series_title`; the observed Gold result had 337 unique series and no missing titles.

**Q1 BLS and population:** Filter to `PRS30006032` and `Q01`; `LEFT JOIN` on `year` so earlier or unmatched BLS observations still appear with `NULL` population. The series title was verified as “Hours worked for the Manufacturing sector, % Change from previous quarter.”

**Validation:** Observed initial Silver counts were 138,487 validated BLS rows and 92,328 deduplicated observations; 46,159 duplicate rows had identical values with zero conflicts. All three SQL vs. PySpark result comparisons passed after the decimal change. The isolated source-ingestion tests passed for first run, unchanged, modified, added and removed files; subsequently, seven offline unit tests on the refactored module also passed.

## 3. Technical trade-offs and improvements for a client

1. **Accuracy vs. input format:** BLS raw numbers are read as strings in Bronze, then `DOUBLE` in Silver. For best-year ranking, `DECIMAL(20,6)` is used for summation and tie-breaking; this fixed a reproducibility issue caused by floating-point sums. For a production client I would parse `DECIMAL` directly from the Bronze raw strings and validate the source's documented precision, rather than casting via `DOUBLE`. The chosen six-decimal scale has not yet been verified against every raw source value.
2. **Correctness vs. refresh cost:** Silver materialized views derive current state from a manifest rather than creating append-only results. With expectations, these views can require full refreshes, which is fine for the present data volume but potentially costly at scale. For larger volumes, I would evaluate CDC, change data feed, incremental merge patterns and expectation monitoring separately.
3. **Input freshness vs. bandwidth:** Every run downloads BLS data again to recompute hashes, but unchanged content is not re-stored or reprocessed by file ingestion. For larger sources, I would implement conditional HTTP requests where supported (`ETag`, `If-Modified-Since`) and consider per-file checkpointing.
4. **Durability and concurrent execution:** Manifest publication occurs only after a complete successful ingestion run, but the prototype is designed for a single writer. It is not a transactional coordination system for concurrent jobs. For a production deployment I would use an orchestrated single-writer job, transactional manifest/status storage and alerts for failures or unexpected source inventory shrinkage.
5. **Schema drift:** The current expected BLS schemas and population JSON structure are explicit. The ingestion validates that the population API has essential fields. For clients I would also quarantine malformed records, version schema contracts, alert on unexpected column changes, and test sample records across all BLS metadata files.
6. **Security and operations:** The BLS contact email is stored outside GitHub; actual secrets would be in Databricks Secrets or a managed secret store. Production would use service principals, least-privilege Unity Catalog grants, monitoring, run-level metrics, alerting, storage-retention policy, and a deployment bundle for environment promotion.
7. **Scope of evidence:** Offline tests verify the source manifest update rules. The original pipeline and its analytical outputs were exercised in Databricks, but an isolated end-to-end add/change/remove replay through the **refactored** ingestion plus a live SDP refresh has not yet been demonstrated; this remains the key follow-up integration test.

## 4. Retrospective

Two problems required more investigation than the happy path:

- **Nested Delta column name:** Data USA returns `Nation ID` with a space. The Bronze nested array initially failed Delta name validation. I mapped the nested fields into normalized struct names while retaining the original JSON bytes in the Volume.
- **Floating-point ranking discrepancy:** Spark SQL and PySpark initially picked different best years for two BLS series even though their displayed annual sums matched. The cause was floating-point aggregation affecting year ranking. I changed both versions to sum as `DECIMAL(20,6)` before applying `ROW_NUMBER() ORDER BY yearly_score DESC, year ASC`. After the change all three cross-implementation comparisons passed, and these tie cases were explicitly checked.

The other notable design issue was the overlap between `pr.data.0.Current` and `pr.data.1.AllData`. Keeping both raw files honored the sourcing requirement, but Silver had to deduplicate on the business key rather than treating every file row as unique.

## 5. AI usage disclosure

I used **ChatGPT** as a reference and coding assistant for pipeline scaffolding, BLS/API ingestion, Spark SQL and PySpark implementations, diagnostic queries, testing, and documentation structure. I ran and debugged the pipeline in Databricks, inspected its output, supplied validation results, and iterated on issues (including nested JSON names and the decimal tie-breaking fix). A later refactor split the notebook code into reusable Python modules and independent tests; those local tests passed, but a successful live run of that refactored ingestion module is not yet documented. I will review and be prepared to explain every submitted line of code and design decision.

## 6. Remaining submission checklist

- [ ] Import the refactored Python ingestion module into Databricks and run it against the existing Volume; verify no new snapshot for unchanged data.
- [ ] Confirm SQL expectations' pipeline quality metrics, including any dropped-row counts.
- [ ] Check raw BLS numeric precision fits the decimal casting assumption or revise Silver to parse decimal directly.
- [ ] Optionally run a controlled end-to-end manifest change against a **separate test Volume/pipeline**, not the working production data.
- [x] Capture actual Databricks pipeline + Gold screenshots in `screenshots/`.
- [ ] Verify final GitHub files and share the repository URL with Rearc.
