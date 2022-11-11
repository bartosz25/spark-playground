from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

rdd1 = spark.sparkContext.parallelize([(1), (2), (3)])
rdd2 = spark.sparkContext.parallelize([(10), (20), (30)])

rdd1.cartesian(rdd2).collect()