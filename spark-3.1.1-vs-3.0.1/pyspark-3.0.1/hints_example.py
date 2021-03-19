from pyspark import Row
from pyspark.sql import SparkSession

User = Row("name")

# Type errors detection at runtime
spark_session = SparkSession.builder.master(User("a"))

df = spark_session.createDataFrame([
    User("a"), User("b"), User("c"), User("d")
])
# type CTRL+space to see the autocomplete list, completely not appropriate!
df.