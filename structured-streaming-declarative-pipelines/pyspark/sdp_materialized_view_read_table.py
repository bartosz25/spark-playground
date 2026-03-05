from pyspark.sql.connect.session import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

print('in_memory_numbers')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/in_memory_numbers').show()

print('text_letters_materialized_view')
spark.read.format('delta').load(path='/opt/spark/sbin/spark-warehouse/text_letters_materialized_view').show()