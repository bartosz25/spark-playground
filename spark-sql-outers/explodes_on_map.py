from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

letters = spark.createDataFrame([
    {'id': 1, 'letters': {'a': 'A','b': 'B','c': 'C'}},
    {'id': 2, 'letters': None},
    {'id': 3, 'letters': {'d': None}},
], 'id INT, letters MAP<STRING, STRING>')
(letters.select(
    F.col('id'), F.col('letters'),
    F.explode('letters').alias('lower', 'upper'))
 .show(truncate=False))

(letters.select(
    F.col('id'), F.col('letters'),
    F.explode_outer('letters').alias('lower', 'upper'))
 .show(truncate=False))