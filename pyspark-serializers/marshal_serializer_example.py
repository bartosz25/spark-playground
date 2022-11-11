from pyspark import SparkContext, MarshalSerializer
from pyspark.sql import SparkSession

spark_context = SparkContext(serializer=MarshalSerializer())
spark_session = SparkSession.builder.master("local[1]").getOrCreate()
rdd1 = spark_session.sparkContext.parallelize([(1), (2), (3)], numSlices=2)
rdd1.collect()