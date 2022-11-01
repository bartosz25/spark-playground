from pyspark.sql import SparkSession
from pyspark.sql.functions import udf;


spark_session = SparkSession.builder.master("local[*]").getOrCreate()

spark_session.range(10).select(udf(lambda x: x/0)("id")).collect()

