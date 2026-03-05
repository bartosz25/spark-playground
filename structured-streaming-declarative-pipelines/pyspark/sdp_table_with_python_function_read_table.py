from pyspark.sql.connect.session import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

print('rate_data_with_processing_time')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_data_with_processing_time').show(truncate=False)
