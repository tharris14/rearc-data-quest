"""Independent PySpark DataFrame implementations of all three Gold questions.

Production Gold is authored in pipeline/03_gold.sql. These functions intentionally
rebuild results from Silver, not from the SQL Gold materialized views.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

QUARTERS = ('Q01', 'Q02', 'Q03', 'Q04')


def population_statistics(population: DataFrame) -> DataFrame:
    return (
        population
        .filter((F.col('year').between(2013, 2018)) & (F.col('nation_id') == '01000US'))
        .agg(
            F.min('year').alias('start_year'),
            F.max('year').alias('end_year'),
            F.count('*').alias('years_observed'),
            F.avg('population').alias('mean_population'),
            F.stddev_pop('population').alias('stddev_population'),
            F.stddev_samp('population').alias('stddev_sample'),
        )
    )


def best_year_per_series(bls: DataFrame, series: DataFrame) -> DataFrame:
    frequencies = (
        bls.groupBy('series_id')
        .agg(F.max(
            F.when(F.col('period').isin(*QUARTERS), 1).otherwise(0)
        ).alias('has_quarterly'))
    )
    included = (
        bls.join(frequencies, on='series_id')
        .filter(
            ((F.col('has_quarterly') == 1) & F.col('period').isin(*QUARTERS))
            | ((F.col('has_quarterly') == 0) & (F.col('period') == 'Q05'))
        )
    )
    yearly = (
        included.groupBy('series_id', 'year', 'has_quarterly')
        .agg(
            F.sum(F.col('value').cast('decimal(20,6)')).alias('yearly_score'),
            F.count('*').alias('periods_included'),
        )
        .withColumn('calculation_method', F.when(
            F.col('has_quarterly') == 1, F.lit('quarterly_sum')
        ).otherwise(F.lit('annual_q05')))
    )
    ranking = Window.partitionBy('series_id').orderBy(
        F.col('yearly_score').desc(), F.col('year').asc()
    )
    ranked = yearly.withColumn('year_rank', F.row_number().over(ranking)).filter('year_rank = 1')
    return (
        ranked.join(series.select('series_id', 'series_title'), on='series_id', how='left')
        .select(
            'series_id', 'series_title', F.col('year').alias('best_year'),
            F.col('yearly_score').cast('double').alias('yearly_score'),
            'periods_included', 'calculation_method',
        )
    )


def bls_population_q1(bls: DataFrame, population: DataFrame) -> DataFrame:
    b = bls.filter(
        (F.col('series_id') == 'PRS30006032') & (F.col('period') == 'Q01')
    ).alias('b')
    p = population.filter(F.col('nation_id') == '01000US').alias('p')
    return (
        b.join(p, F.col('b.year') == F.col('p.year'), how='left')
        .select(
            F.col('b.series_id').alias('series_id'),
            F.col('b.year').alias('year'),
            F.col('b.period').alias('period'),
            F.col('b.value').alias('bls_value'),
            F.col('p.population').alias('us_population'),
            F.col('b.source_file_path').alias('source_file_path'),
        )
    )


def load_alternative_outputs(spark, schema: str = 'workspace.rearc_quest') -> dict[str, DataFrame]:
    """Load Silver inputs and return independently calculated PySpark results."""
    population = spark.table(f'{schema}.silver_population')
    bls = spark.table(f'{schema}.silver_bls_observations')
    series = spark.table(f'{schema}.silver_bls_series')
    return {
        'gold_population_statistics': population_statistics(population),
        'gold_bls_best_year': best_year_per_series(bls, series),
        'gold_bls_population_join': bls_population_q1(bls, population),
    }
