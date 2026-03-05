from pyspark.sql.connect.session import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

print('rate_table_append_flow')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_table_append_flow').show(truncate=False)
