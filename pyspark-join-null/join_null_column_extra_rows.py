from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

# Replaced None by 'None' to easily see the generated join in case a database would consider NULLs as values
letters_1 = spark.createDataFrame([{'id': '1', 'letter': 'a'}, {'id': 'None', 'letter': 'b'}, {'id': '3', 'letter': 'c'}],
                                  'id STRING, letter STRING')
letters_2 = spark.createDataFrame([{'id': '1', 'letter': 'A'}, {'id': 'None', 'letter': 'B'}, {'id': '4', 'letter': 'D'},
                                   {'id': 'None', 'letter': 'E'}, {'id': 'None', 'letter': 'F'}],
                                  'id STRING, letter STRING')

letters_1.join(letters_2, on=['id'], how='full_outer').show()