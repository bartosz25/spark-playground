from decimal import Decimal

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master('local[*]').getOrCreate()


numbers = [
    {'number_1': 1.49, 'number_2': 2.33, 'number_1_dec': Decimal('1.49'), 'number_2_dec': Decimal('2.33')}
]
numbers_df = spark.createDataFrame(numbers, 'number_1 DOUBLE, number_2 DOUBLE, number_1_dec DECIMAL(5, 3), number_2_dec DECIMAL(5, 3)')

numbers_df_multiplied = (numbers_df.withColumn('multiplication_double', F.col('number_1') * F.col('number_2'))
                                              .withColumn('multiplication_dec', F.col('number_1_dec') * F.col('number_2_dec')))

numbers_df_multiplied.printSchema()
numbers_df_multiplied.show(truncate=False)

"""
root
 |-- number_1: double (nullable = true)
 |-- number_2: double (nullable = true)
 |-- number_1_dec: decimal(5,3) (nullable = true)
 |-- number_2_dec: decimal(5,3) (nullable = true)
 |-- multiplication_double: double (nullable = true)
 |-- multiplication_dec: decimal(11,6) (nullable = true)
 
 +--------+--------+------------+------------+---------------------+------------------+
|number_1|number_2|number_1_dec|number_2_dec|multiplication_double|multiplication_dec|
+--------+--------+------------+------------+---------------------+------------------+
|1.49    |2.33    |1.490       |2.330       |3.4717000000000002   |3.471700          |
+--------+--------+------------+------------+---------------------+------------------+

"""

