from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters_1 = spark.createDataFrame([{'id': 1, 'letter': 'a'}, {'id': None, 'letter': 'b'}, {'id': 3, 'letter': 'c'}],
                                  'id INT, letter STRING')
letters_2 = spark.createDataFrame([{'id': 1, 'letter': 'A'}, {'id': None, 'letter': 'B'}, {'id': 4, 'letter': 'D'},
                                   {'id': None, 'letter': 'E'}, {'id': None, 'letter': 'F'}],
                                  'id INT, letter STRING')
letters_1.createOrReplaceTempView('letters_1')
letters_2.createOrReplaceTempView('letters_2')

print('Version with IS NULL')
spark.sql('''
SELECT letters_1.id, letters_1.letter AS letter1, letters_2.letter AS letter2
FROM letters_1 
FULL OUTER JOIN letters_2 ON letters_1.id = letters_2.id OR (letters_1.id IS NULL AND letters_2.id IS NULL)
''').show()

print('Version with COALESCE')
spark.sql('''
SELECT letters_1.id, letters_1.letter AS letter1, letters_2.letter AS letter2
FROM letters_1 
FULL OUTER JOIN letters_2 ON COALESCE(letters_1.id, "not_existing_id") = COALESCE(letters_2.id, "not_existing_id")
''').show()

spark.sql('SELECT COUNT(*) FROM letters_2').show()
spark.sql('SELECT COUNT(id) FROM letters_2').show()