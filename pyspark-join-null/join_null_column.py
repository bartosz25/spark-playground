from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters_1 = spark.createDataFrame([{'id': 1, 'letter': 'a'}, {'id': None, 'letter': 'b'}, {'id': 3, 'letter': 'c'}],
                                  'id INT, letter STRING')
letters_2 = spark.createDataFrame([{'id': 1, 'letter': 'A'}, {'id': None, 'letter': 'B'}, {'id': 4, 'letter': 'D'}],
                                  'id INT, letter STRING')

letters_1.join(letters_2, on=['id'], how='full_outer').show()