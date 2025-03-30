from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters = spark.createDataFrame([{'id': 1, 'letter': 'A'}, {'id': None, 'letter': 'B'}, {'id': 4, 'letter': 'D'},
                                   {'id': None, 'letter': 'E'}, {'id': None, 'letter': 'F'}],
                                  'id INT, letter STRING')
letters.createOrReplaceTempView('letters')

spark.sql('SELECT COUNT(*) AS all_rows FROM letters').show()
spark.sql('SELECT COUNT(id) AS all_rows FROM letters').show()
spark.sql('SELECT AVG(id) AS average_id FROM letters').show()
