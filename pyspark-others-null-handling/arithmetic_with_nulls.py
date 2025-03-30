from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters = spark.createDataFrame([{'id': 1, 'letter': 'A'}, {'id': None, 'letter': 'B'}, {'id': 4, 'letter': 'D'},
                                   {'id': None, 'letter': 'E'}, {'id': None, 'letter': 'F'}],
                                  'id INT, letter STRING')
letters.createOrReplaceTempView('letters')

spark.sql('SELECT id, id + 3 AS id_sum, CONCAT_WS("_", "id", id) AS id_concat FROM letters').show()
