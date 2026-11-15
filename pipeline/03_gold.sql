
CREATE OR REFRESH MATERIALIZED VIEW gold_population_statistics
COMMENT 'US population mean and standard deviation, 2013-2018'
AS
SELECT
    MIN(year) AS start_year,
    MAX(year) AS end_year,
    COUNT(*) AS years_observed,
    AVG(population) AS mean_population,
    STDDEV_POP(population) AS stddev_population,
    STDDEV_SAMP(population) AS stddev_sample
FROM silver_population
WHERE year BETWEEN 2013 AND 2018
  AND nation_id = '01000US';


CREATE OR REFRESH MATERIALIZED VIEW gold_bls_best_year
COMMENT 'Best year per BLS series, with explicit annual-only handling'
AS
WITH series_frequency AS (
    SELECT
        series_id,
        MAX(
            CASE
                WHEN period IN ('Q01','Q02','Q03','Q04')
                THEN 1 ELSE 0
            END
        ) AS has_quarterly
    FROM silver_bls_observations
    GROUP BY series_id
),

yearly_scores AS (
    SELECT
        o.series_id,
        o.year,
        SUM(CAST(o.value AS DECIMAL(20, 6))) AS yearly_score,
        COUNT(*) AS periods_included,
        CASE
            WHEN f.has_quarterly = 1
            THEN 'quarterly_sum'
            ELSE 'annual_q05'
        END AS calculation_method
    FROM silver_bls_observations o
    INNER JOIN series_frequency f
        ON o.series_id = f.series_id
    WHERE
        (
            f.has_quarterly = 1
            AND o.period IN ('Q01','Q02','Q03','Q04')
        )
        OR
        (
            f.has_quarterly = 0
            AND o.period = 'Q05'
        )
    GROUP BY
        o.series_id,
        o.year,
        f.has_quarterly
),

ranked_years AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY yearly_score DESC, year ASC
        ) AS year_rank
    FROM yearly_scores
)

SELECT
    r.series_id,
    s.series_title,
    r.year AS best_year,
    CAST(r.yearly_score AS DOUBLE) AS yearly_score,
    r.periods_included,
    r.calculation_method
FROM ranked_years r
LEFT JOIN silver_bls_series s
    ON r.series_id = s.series_id
WHERE r.year_rank = 1;


CREATE OR REFRESH MATERIALIZED VIEW gold_bls_population_join
COMMENT 'Q1 manufacturing hours worked change with US population'
AS
SELECT
    b.series_id,
    b.year,
    b.period,
    b.value AS bls_value,
    p.population AS us_population,
    b.source_file_path
FROM silver_bls_observations b
LEFT JOIN silver_population p
    ON b.year = p.year
    AND p.nation_id = '01000US'
WHERE b.series_id = 'PRS30006032'
  AND b.period = 'Q01';
