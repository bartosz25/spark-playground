from pyspark import Row
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.master("local[*]") \
    .appName("New features") \
    .getOrCreate()

# Catalog functions
assert not spark_session.catalog.functionExists("not_existing_fn", "default")
spark_session.sql("CREATE FUNCTION fn AS 'com.waitingforcode.fn'")
assert spark_session.catalog.functionExists("fn", "default")

# Schema inference
dataframe_as_dict = [{"nested": {"city": "Paris", "country": "France"}}]
cities_from_dict = spark_session.createDataFrame(dataframe_as_dict)
cities_from_dict.show(truncate=False)
"""
+----------------------------------+
|nested                            |
+----------------------------------+
|{country -> France, city -> Paris}|
+----------------------------------+
"""

# Python's standard string formatter
User = Row("name")

users_df = spark_session.createDataFrame([
    User("a"), User("b"), User("c"), User("d")
])

spark_session.sql("SELECT * FROM {table_from_dataframe}", table_from_dataframe=users_df).show(truncate=False)


