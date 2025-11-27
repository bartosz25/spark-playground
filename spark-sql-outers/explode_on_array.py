from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters = spark.createDataFrame([{'id': 1, 'letters': ['a','b','c']}], 'id INT, letters ARRAY<STRING>')
letters.withColumn('letter', F.explode('letters')).sort(F.asc('letter')).show(truncate=False)


letters_with_nulls = spark.createDataFrame([
    {'id': 1, 'letters': ['a','b','c']}, {'id': 2, 'letters': None}, {'id': 3, 'letters': ['d',None]}
], 'id INT, letters ARRAY<STRING>')
letters_with_nulls.withColumn('letter', F.explode('letters')).sort(F.asc('letter')).show(truncate=False)