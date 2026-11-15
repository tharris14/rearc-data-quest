"""Databricks validation: compare SQL Gold to PySpark implementations using Silver.

Run as a Databricks notebook script / driver program in a session with the pipeline
already refreshed. This integration validation needs a live Databricks workspace.
"""

import math

from pyspark.sql import functions as F
from alternatives.pyspark_alternatives import load_alternative_outputs

SCHEMA = 'workspace.rearc_quest'


def validate(spark, schema: str = SCHEMA) -> None:
    outputs = load_alternative_outputs(spark, schema)
    sql_pop = spark.table(f'{schema}.gold_population_statistics')
    sql_best = spark.table(f'{schema}.gold_bls_best_year')
    sql_join = spark.table(f'{schema}.gold_bls_population_join')
    py_pop = outputs['gold_population_statistics']
    py_best = outputs['gold_bls_best_year']
    py_join = outputs['gold_bls_population_join']

    sql_row, py_row = sql_pop.first().asDict(), py_pop.first().asDict()
    for key in ('start_year', 'end_year', 'years_observed'):
        assert sql_row[key] == py_row[key], f'Population mismatch: {key}'
    for key in ('mean_population', 'stddev_population', 'stddev_sample'):
        assert math.isclose(sql_row[key], py_row[key], rel_tol=1e-10, abs_tol=1e-4), key
    assert sql_row['years_observed'] == 6
    print('PASS: Population statistics')

    columns = ['series_id', 'series_title', 'best_year', 'periods_included', 'calculation_method']
    assert (sql_best.select(*columns).exceptAll(py_best.select(*columns)).count() == 0 and
            py_best.select(*columns).exceptAll(sql_best.select(*columns)).count() == 0)
    comparison = (
        sql_best.select('series_id', F.col('yearly_score').alias('sql_score'))
        .join(py_best.select('series_id', F.col('yearly_score').alias('py_score')),
              on='series_id', how='full_outer')
    )
    assert comparison.filter(
        F.col('sql_score').isNull() | F.col('py_score').isNull()
        | (F.abs(F.col('sql_score') - F.col('py_score')) > 1e-8)
    ).count() == 0
    assert sql_best.count() == 337
    print('PASS: Best year per BLS series')

    columns = ['series_id', 'year', 'period', 'bls_value', 'us_population', 'source_file_path']
    assert (sql_join.select(*columns).exceptAll(py_join.select(*columns)).count() == 0 and
            py_join.select(*columns).exceptAll(sql_join.select(*columns)).count() == 0)
    print('PASS: BLS/population Q1 join')
    print('All three SQL/PySpark comparisons passed.')


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    validate(SparkSession.builder.getOrCreate())
