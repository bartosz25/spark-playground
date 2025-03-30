from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters = spark.createDataFrame([{'id': 1, 'letter': 'A'}, {'id': None, 'letter': 'B'}, {'id': 4, 'letter': 'D'},
                                   {'id': None, 'letter': 'E'}, {'id': None, 'letter': 'F'}],
                                  'id INT, letter STRING')
letters.createOrReplaceTempView('letters')

spark.sql('SELECT * FROM letters ORDER BY id ASC NULLS LAST').show()
spark.sql('SELECT * FROM letters ORDER BY id ASC NULLS FIRST').show()
