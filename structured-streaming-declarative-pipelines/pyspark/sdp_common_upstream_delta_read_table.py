from pyspark.sql import functions as F, DataFrame, SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

def _show_data(df: DataFrame):
    df.selectExpr("CAST(value AS STRING)").orderBy(F.desc('value')).show(truncate=False, n=10)

print('rate_sink_1_delta_table')
_show_data(
    spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_1_delta_table')
)
print('rate_sink_2_delta_table')
_show_data(
    spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_2_delta_table')
)

print(f'''Counts:
      {spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_1_delta_table').count()}
      vs .
      {spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_2_delta_table').count()}
''')