
CREATE OR REFRESH STREAMING TABLE bronze_bls_observations
COMMENT 'Raw BLS productivity observations with source-file lineage'
AS
SELECT
    series_id AS series_id_raw,
    year AS year_raw,
    period AS period_raw,
    value AS value_raw,
    footnote_codes AS footnote_codes_raw,
    _metadata.file_path AS source_file_path,
    _metadata.file_modification_time AS source_modified_at
FROM STREAM read_files(
    '/Volumes/workspace/rearc_quest/source_files/raw/bls/pr.data.*',
    format => 'csv',
    delimiter => '\t',
    header => true,
    schema => '
        series_id STRING,
        year STRING,
        period STRING,
        value STRING,
        footnote_codes STRING
    ',
    recursiveFileLookup => true,
    enforceSchema => true
);


CREATE OR REFRESH STREAMING TABLE bronze_bls_series
COMMENT 'Raw BLS series metadata and human-readable labels'
AS
SELECT
    series_id AS series_id_raw,
    sector_code,
    class_code,
    measure_code,
    duration_code,
    seasonal,
    base_year,
    footnote_codes,
    series_title,
    begin_year,
    begin_period,
    end_year,
    end_period,
    _metadata.file_path AS source_file_path
FROM STREAM read_files(
    '/Volumes/workspace/rearc_quest/source_files/raw/bls/pr.series/',
    format => 'csv',
    delimiter => '\t',
    header => true,
    schema => '
        series_id STRING,
        sector_code STRING,
        class_code STRING,
        measure_code STRING,
        duration_code STRING,
        seasonal STRING,
        base_year STRING,
        footnote_codes STRING,
        series_title STRING,
        begin_year STRING,
        begin_period STRING,
        end_year STRING,
        end_period STRING
    ',
    recursiveFileLookup => true,
    enforceSchema => true
);



CREATE OR REFRESH STREAMING TABLE bronze_population
COMMENT 'Raw population API records with standardized field names'
AS
SELECT
    TRANSFORM(
        data,
        x -> NAMED_STRUCT(
            'nation_id', x.`Nation ID`,
            'nation', x.Nation,
            'year', x.Year,
            'population', x.Population
        )
    ) AS population_records,
    _metadata.file_path AS source_file_path
FROM STREAM read_files(
    '/Volumes/workspace/rearc_quest/source_files/raw/data_usa/population.json/',
    format => 'json',
    multiLine => true,
    schema => '
        data ARRAY<STRUCT<
            `Nation ID`: STRING,
            Nation: STRING,
            Year: INT,
            Population: DOUBLE
        >>
    ',
    recursiveFileLookup => true
);

CREATE OR REFRESH STREAMING TABLE bronze_manifest_snapshots
COMMENT 'History of source inventories published by Python ingestion'
AS
SELECT
    run_id,
    created_at_utc,
    active_files,
    removed_since_previous,
    _metadata.file_path AS manifest_file_path
FROM STREAM read_files(
    '/Volumes/workspace/rearc_quest/source_files/manifests/',
    format => 'json',
    multiLine => true,
    schema => '
        run_id STRING,
        created_at_utc STRING,
        active_files ARRAY<STRUCT<
            source: STRING,
            filename: STRING,
            source_url: STRING,
            sha256: STRING,
            volume_path: STRING,
            size_bytes: BIGINT
        >>,
        removed_since_previous ARRAY<STRUCT<
            source: STRING,
            filename: STRING
        >>
    ',
    recursiveFileLookup => true
);
