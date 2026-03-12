import datetime
import random
import time

from pyspark import pipelines, Row
from pyspark.sql import SparkSession

@pipelines.materialized_view(format='delta')
def in_memory_numbers_2():
    spark = SparkSession.active()
    old_letters = spark.read.table('in_memory_numbers_temporary_view')

    new_letters = spark.createDataFrame(
        data=[
            Row(id=1, letter='A'), Row(id=2, letter='B'), Row(id=3, letter='C')
        ]
    )

    return old_letters.unionByName(new_letters, allowMissingColumns=False)


@pipelines.materialized_view(format='delta')
def in_memory_numbers_1():
    spark = SparkSession.active()
    old_letters = spark.read.table('in_memory_numbers_temporary_view')

    new_letters = spark.createDataFrame(
        data=[
            Row(id=1, letter='A'), Row(id=2, letter='B'), Row(id=3, letter='C')
        ]
    )

    return old_letters.unionByName(new_letters, allowMissingColumns=False)

@pipelines.temporary_view()
def in_memory_numbers_temporary_view():
    spark = SparkSession.active()
    sleep_time = random.randint(0, 20)
    print('⏰ Sleeping for', sleep_time, 'seconds')
    time.sleep(sleep_time)
    letter_prefix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return spark.createDataFrame(
        data=[
            Row(id=4, letter=f'{letter_prefix}_D'),
            Row(id=5, letter=f'{letter_prefix}_E'),
            Row(id=6, letter=f'{letter_prefix}_F')
        ]
    )
