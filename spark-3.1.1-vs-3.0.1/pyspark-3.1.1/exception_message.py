from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import udf;


spark_session = SparkSession.builder.master("local[*]")\
    .config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "true").getOrCreate()

spark_session.range(10).select(udf(lambda x: x/0)("id")).collect()