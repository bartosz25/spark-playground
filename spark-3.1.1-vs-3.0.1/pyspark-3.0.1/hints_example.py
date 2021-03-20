from pyspark import Row
from pyspark.sql import SparkSession

User = Row("name")

spark_session = SparkSession.builder.master("local").getOrCreate()

df = spark_session.createDataFrame([
    User("a"), User("b"), User("c"), User("d")
])

df.sh
