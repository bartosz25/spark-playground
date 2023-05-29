import pyspark.sql.connect.session
from pyspark.sql import SparkSession

# replace the remote address by your instance
spark_session = SparkSession.builder.remote("sc://localhost").getOrCreate()

print(type(spark_session))

assert type(spark_session) == pyspark.sql.connect.session.SparkSession

dataframe = spark_session.createDataFrame([('a'), ('b'), ('c'), ('d')], ['letter'])

dataframe = spark_session.read.text("/tmp/some_text_files")
print(type(dataframe))

assert str(type(dataframe)) == "<class 'pyspark.sql.connect.dataframe.DataFrame'>"

dataframe.collect()