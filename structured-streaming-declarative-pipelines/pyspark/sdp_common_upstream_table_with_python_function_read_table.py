from pyspark.sql.connect.session import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

print('rate_sink_1')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_1').show()

print('rate_sink_2')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/rate_sink_2').show()
