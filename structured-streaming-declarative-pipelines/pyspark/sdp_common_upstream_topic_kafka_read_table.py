from pyspark.sql import functions as F, DataFrame, SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

def _show_data(df: DataFrame):
    df.selectExpr("CAST(value AS STRING)").orderBy(F.desc('value')).show(truncate=False, n=10)

print('rate_sink_1')
_show_data(
    spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_1')
)
print('rate_sink_2')
_show_data(
    spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_2')
)