from pyspark import Row
from pyspark.sql import SparkSession

User = Row("name")

# Type errors detection at runtime
spark_session = SparkSession.builder.master(User("a", 20))

df = spark_session.createDataFrame([
    User("a", 49), User("b", 20), User("c", 20), User("d", 41)
])

# type CTRL+space to see the autocomplete list, completely not appropriate!
df.

