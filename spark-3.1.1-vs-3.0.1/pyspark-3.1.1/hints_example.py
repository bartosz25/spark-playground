from pyspark import Row
from pyspark.sql import SparkSession

User = Row("name", "age")

spark_session = SparkSession.builder.master(User("d", 4)).getOrCreate()

df = spark_session.createDataFrame([
    User("a", 1), User("b", 2), User("c", 3), User("d", 4)
])

df.show(truncate=False, vertical=True)
