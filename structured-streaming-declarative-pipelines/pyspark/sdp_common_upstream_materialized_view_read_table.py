from pyspark.sql.connect.session import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

print('in_memory_numbers_1')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/in_memory_numbers_1').show()

print('in_memory_numbers_2')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/in_memory_numbers_2').show()
