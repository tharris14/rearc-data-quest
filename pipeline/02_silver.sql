
CREATE OR REFRESH MATERIALIZED VIEW current_source_files
COMMENT 'Files belonging to the latest successful ingestion'
AS
WITH ranked_manifests AS (
    SELECT
        active_files,
        ROW_NUMBER() OVER (
            ORDER BY created_at_utc DESC, run_id DESC
        ) AS rn
    FROM bronze_manifest_snapshots
)

SELECT
    file.source,
    file.filename,
    file.sha256,
    file.volume_path
FROM ranked_manifests r
LATERAL VIEW EXPLODE(r.active_files) e AS file
WHERE r.rn = 1;


CREATE OR REFRESH MATERIALIZED VIEW silver_bls_validated (
    CONSTRAINT valid_series_id
        EXPECT (series_id IS NOT NULL AND series_id <> '')
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_year
        EXPECT (year BETWEEN 1900 AND 2100)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_period
        EXPECT (period IN ('Q01','Q02','Q03','Q04','Q05'))
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_numeric_value
        EXPECT (value IS NOT NULL AND NOT ISNAN(value))
        ON VIOLATION DROP ROW
)
COMMENT 'Typed and validated BLS observations before deduplication'
AS
SELECT
    NULLIF(TRIM(b.series_id_raw), '') AS series_id,
    TRY_CAST(TRIM(b.year_raw) AS INT) AS year,
    TRIM(b.period_raw) AS period,
    TRY_CAST(TRIM(b.value_raw) AS DOUBLE) AS value,
    NULLIF(TRIM(b.footnote_codes_raw), '') AS footnote_codes,
    b.source_file_path
FROM bronze_bls_observations b
INNER JOIN current_source_files a
    ON REGEXP_REPLACE(b.source_file_path, '^dbfs:', '')
       = a.volume_path
WHERE a.source = 'bls'
  AND a.filename LIKE 'pr.data.%';


CREATE OR REFRESH MATERIALIZED VIEW silver_bls_observations
COMMENT 'One validated BLS observation per series, year, and period'
AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, year, period
            ORDER BY
                CASE
                    WHEN source_file_path LIKE
                        '%/pr.data.0.Current/%'
                    THEN 0
                    ELSE 1
                END,
                source_file_path DESC
        ) AS rn
    FROM silver_bls_validated
)

SELECT
    series_id,
    year,
    period,
    value,
    footnote_codes,
    source_file_path
FROM ranked
WHERE rn = 1;


CREATE OR REFRESH MATERIALIZED VIEW silver_bls_series (
    CONSTRAINT valid_series_key
        EXPECT (series_id IS NOT NULL AND series_id <> '')
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_series_title
        EXPECT (series_title IS NOT NULL AND series_title <> '')
        ON VIOLATION DROP ROW
)
COMMENT 'Clean BLS series reference data with descriptive titles'
AS
SELECT
    NULLIF(TRIM(b.series_id_raw), '') AS series_id,
    NULLIF(TRIM(b.series_title), '') AS series_title,
    TRIM(b.sector_code) AS sector_code,
    TRIM(b.class_code) AS class_code,
    TRIM(b.measure_code) AS measure_code,
    TRIM(b.duration_code) AS duration_code,
    TRIM(b.seasonal) AS seasonal,
    TRY_CAST(TRIM(b.begin_year) AS INT) AS begin_year,
    TRIM(b.begin_period) AS begin_period,
    TRY_CAST(TRIM(b.end_year) AS INT) AS end_year,
    TRIM(b.end_period) AS end_period,
    b.source_file_path
FROM bronze_bls_series b
INNER JOIN current_source_files a
    ON REGEXP_REPLACE(b.source_file_path, '^dbfs:', '')
       = a.volume_path
WHERE a.source = 'bls'
  AND a.filename = 'pr.series';


CREATE OR REFRESH MATERIALIZED VIEW silver_population (
    CONSTRAINT valid_population_key
        EXPECT (nation_id IS NOT NULL AND year IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_population
        EXPECT (
            population IS NOT NULL
            AND population > 0
            AND population = FLOOR(population)
        )
        ON VIOLATION DROP ROW
)
COMMENT 'Validated annual US population with one record per year'
AS
WITH flattened AS (
    SELECT
        EXPLODE(population_records) AS record,
        source_file_path
    FROM bronze_population
),

typed AS (
    SELECT
        f.record.nation_id AS nation_id,
        f.record.nation AS nation,
        f.record.year AS year,
        f.record.population AS population,
        f.source_file_path
    FROM flattened f
    INNER JOIN current_source_files a
        ON REGEXP_REPLACE(f.source_file_path, '^dbfs:', '')
           = a.volume_path
    WHERE a.source = 'data_usa'
      AND a.filename = 'population.json'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY nation_id, year
            ORDER BY source_file_path DESC
        ) AS rn
    FROM typed
    WHERE nation_id = '01000US'
)

SELECT
    nation_id,
    nation,
    year,
    population,
    source_file_path
FROM ranked
WHERE rn = 1;


