# Rearc Data Quest — Databricks Edition

A Databricks Spark Declarative Pipeline that ingests Bureau of Labor Statistics (BLS) productivity data and USA annual population records, then produces trusted Bronze, Silver, and Gold Delta datasets. The **Gold analytical questions are implemented in Spark SQL**, with **independent PySpark alternatives** and result-comparison tests.

> Status: the Databricks pipeline ran successfully, all three SQL/PySpark comparisons passed (including the follow-up comparison in the Git-backed workspace), and five ingestion resilience scenarios passed in notebook tests. The refactored Python module passes seven offline unit tests and a live Databricks Volume rerun confirmed **13/13 unchanged files, zero new manifest snapshots**. See [PROCESS.md](PROCESS.md) for remaining verification.

## Architecture

```text
BLS productivity directory ──┐
                             ├─ Python source ingestion ─> UC Volume (immutable SHA-256 versions)
Data USA population API ─────┘                            └─ manifest snapshots (active files)
                                                                  │
                                          Spark Declarative Pipeline
                                                                  │
                                      Bronze: raw observations, series metadata,
                                              population JSON, manifests
                                                                  │
                                      Silver: typed/validated/deduplicated data,
                                              latest active-source inventory
                                                                  │
                                      Gold: population statistics,
                                            best year per BLS series,
                                            Q1 BLS + population join
```

## Repository layout

| Path | Purpose |
|---|---|
| `ingestion/ingest.py` | Dynamic source discovery, BLS contact header, retries, checksum/versioning, active-file manifest |
| `pipeline/01_bronze.sql` | Four Bronze streaming tables |
| `pipeline/02_silver.sql` | Active manifest plus validated, deduplicated Silver materialized views |
| `pipeline/03_gold.sql` | Three primary Gold Spark SQL materialized views |
| `alternatives/pyspark_alternatives.py` | Independent DataFrame API implementations of all three questions |
| `tests/test_ingestion.py` | Offline ingestion source-change and failure tests |
| `tests/validate_sql_vs_pyspark.py` | Integration comparison against live Gold tables |
| `PROCESS.md` | Architecture decisions, trade-offs, retrospective, AI disclosure |
| `screenshots/` | Evidence captured from Databricks before submission (see checklist) |

## Setup and execution

**Requirements:** a Databricks workspace with Spark Declarative Pipelines enabled and a Unity Catalog Volume. The original tested environment used the `workspace` catalog, `rearc_quest` schema and `source_files` Volume. Python ingestion uses `requests`; Databricks supplies Spark and Delta.

1. Create the schema and Volume, using a Databricks SQL editor/notebook:

   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.rearc_quest;
   CREATE VOLUME IF NOT EXISTS workspace.rearc_quest.source_files;
   ```

2. Supply a contact email for BLS-compliant HTTP requests **privately**. Use either the environment variable `BLS_CONTACT_EMAIL` or the file `/Volumes/workspace/rearc_quest/source_files/config/local.json` containing the same JSON structure as `config/local.example.json`. Do **not** commit `local.json`, a filled-in example, notebook outputs containing personal information, or tokens. This is a contact identifier, not an API credential; use Databricks Secrets for actual secrets.
3. Install `requirements.txt` in the Python environment where ingestion runs. From the repository root with Volume access, run:

   ```bash
   python -m ingestion.ingest --volume /Volumes/workspace/rearc_quest/source_files
   ```

   In a Databricks Python notebook, after making the repository importable, the equivalent is:

   ```python
   from pathlib import Path
   from ingestion.ingest import load_contact_email, make_session, run_ingestion
   volume = Path('/Volumes/workspace/rearc_quest/source_files')
   with make_session(load_contact_email(volume)) as session:
       run_ingestion(session, volume)
   ```

4. In the Databricks UI, create a **triggered Spark Declarative Pipeline**, with destination catalog `workspace` and schema `rearc_quest`; attach `pipeline/01_bronze.sql`, `pipeline/02_silver.sql`, and `pipeline/03_gold.sql` as source files. Run the ingestion first, then trigger the pipeline. The SQL currently includes the tested Volume path; change it if using another catalog/schema/Volume.
5. Inspect the three Gold views and run the independent PySpark comparison **in the same Databricks environment**:

   ```python
   from tests.validate_sql_vs_pyspark import validate
   validate(spark, schema='workspace.rearc_quest')
   ```

   Ensure the repository root is importable before invoking the script. This check needs access to all materialized Gold and Silver tables.

## Gold output definitions

| Output | Question and rule |
|---|---|
| `gold_population_statistics` | US population mean and population/sample standard deviations for 2013–2018; includes `years_observed = 6` |
| `gold_bls_best_year` | Highest annual sum of `Q01`–`Q04` for quarterly series, earliest year on exact ties; separately labels annual-only `Q05` series |
| `gold_bls_population_join` | `PRS30006032`, period `Q01`, left-joined to annual US population; unmatched years retain null population |

BLS `Q05` is an annual observation and **must not be added to the quarterly sum**. The 54 annual-only series use their `Q05` values for the best-year selection, explicitly labeled `annual_q05`; these scores are not the same aggregation type as `quarterly_sum`.

## Tests and reported results

To run offline ingestion unit tests from the repository root:

```bash
pip install -r requirements-dev.txt
pytest -q
```

The tests mock HTTP responses and use temporary storage; they **do not change the live Volume**. The seven local unit tests pass and cover initial ingestion, unchanged data, changed data, new data, removed data, malformed API response, empty directory, and private configuration (initial + repeat are combined into one test).

The user's original Databricks run reported:

| Validation | Observed |
|---|---:|
| Active files | 13 (12 BLS + one API JSON) |
| Validated BLS observations | 138,487 |
| Deduplicated BLS observations | 92,328 |
| Identical overlapping observations removed | 46,159 |
| Conflicting duplicate values | 0 |
| Series with human-readable titles | 337 / 337 |
| Quarterly series / annual-only series | 283 / 54 |
| Data USA population rows | 11 |
| Gold SQL/PySpark cross-implementation comparisons | 3 passed |

These counts are a **snapshot** and may change as BLS updates the source. The refactored ingestion module was later run directly in Databricks against the existing Volume and found 13 unchanged files with no new snapshot. An inspection of the 138,487 parseable BLS values in Bronze showed a maximum of three decimal places, so the six-decimal Gold aggregation scale is sufficient for this snapshot. Population 2020 was absent from the observed API response; the Q1 join preserves a null rather than imputing it.

## Databricks expectation validation

The latest recorded Spark Declarative Pipeline event log (2026-11-15 21:31 UTC) showed **zero failed and zero dropped rows** for all eight Silver-layer expectations:

| Dataset | Input records | Expectations | Failed records | Dropped records |
|---|---:|---:|---:|---:|
| `silver_bls_validated` | 138,487 | 4 | 0 | 0 |
| `silver_bls_series` | 337 | 2 | 0 | 0 |
| `silver_population` | 11 | 2 | 0 | 0 |

These results demonstrate the current source snapshot passes the defined constraints; a controlled malformed-record replay through the live SDP was not performed.

## Databricks evidence

Four screenshots from the working Databricks workspace are included:

- [Pipeline architecture](screenshots/Pipeline%20Architecture.png) — completed Bronze/Silver/Gold dependency graph
- [Population statistics](screenshots/Population%20Statistics.png) — Gold result for question 1
- [Best year per BLS series](screenshots/Best%20Year%20Per%20BLS%20Series.png) — Gold result for question 2
- [BLS Q1 / population join](screenshots/BLS%20and%20Population%20Join.png) — Gold result for question 3

See [`screenshots/README.md`](screenshots/README.md) for screenshot descriptions.

## Data sources

- [BLS Productivity time-series directory](https://download.bls.gov/pub/time.series/pr/) and [BLS data access policy](https://www.bls.gov/bls/pss.htm)
- [Data USA annual population API](https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population) and [Data USA API documentation](https://datausa.io/about/api/)

See [PROCESS.md](PROCESS.md) for design rationale, data-quality expectations, unresolved limitations, and AI-assistance disclosure.
