from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters_1 = spark.createDataFrame([{'id': 1, 'letter': 'a'}, {'id': None, 'letter': 'b'}, {'id': 3, 'letter': 'c'},
                                   {'id': None, 'letter': 'G'},],
                                  'id INT, letter STRING')
letters_2 = spark.createDataFrame([{'id': 1, 'letter': 'A'}, {'id': None, 'letter': 'B'}, {'id': 4, 'letter': 'D'},
                                   {'id': None, 'letter': 'E'}, {'id': None, 'letter': 'F'},],
                                  'id INT, letter STRING')

letters_1.createOrReplaceTempView('letters_1')
letters_2.createOrReplaceTempView('letters_2')

spark.sql('''
SELECT * FROM letters_1 l1 
WHERE l1.id NOT IN (SELECT id FROM letters_2)
''').show()


spark.sql('''
SELECT * FROM letters_1 l1 
WHERE NOT EXISTS (SELECT 1 FROM letters_2 l2 WHERE l1.id = l2.id)
''').show()