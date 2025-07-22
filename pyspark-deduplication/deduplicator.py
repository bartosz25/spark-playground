from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters = spark.createDataFrame([{'lower_letter': 'a', 'upper_letter': 'A'},
         {'lower_letter': 'b', 'upper_letter': 'B'}, {'lower_letter': 'a', 'upper_letter': 'A'},
         {'lower_letter': 'c', 'upper_letter': 'C'}], 'lower_letter STRING, upper_letter STRING')
letters.createOrReplaceTempView('letters')

spark.sql('''
SELECT dst.unique_letters.* FROM (
    SELECT DISTINCT(lower_letter, upper_letter) AS unique_letters FROM letters
) dst
''').show(truncate=False)
