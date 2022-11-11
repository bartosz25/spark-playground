from pyspark.sql import SparkSession

# FlattenedValuesSerializer
# Set low Python memory to force spilling
spark = SparkSession.builder.master("local[1]")\
    .config("spark.python.worker.memory", "2m").getOrCreate()

rdd = spark.sparkContext.parallelize(range(0, 1000000))
rdd.map(lambda nr: (nr % 2, nr)).groupByKey().mapValues(len).foreachPartition(lambda x: print('a'))

