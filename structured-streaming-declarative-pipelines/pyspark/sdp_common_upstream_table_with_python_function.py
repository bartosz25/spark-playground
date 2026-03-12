import datetime
import random
import time

from pyspark import pipelines, Row
from pyspark.sql import SparkSession, DataFrame, functions as F

@pipelines.table(format='delta')
def rate_sink_1():
    spark = SparkSession.active()
    rate_to_write = spark.readStream.table('in_memory_numbers_temporary_view')
    return rate_to_write


@pipelines.table(format='delta')
def rate_sink_2():
    spark = SparkSession.active()
    rate_to_write = spark.readStream.table('in_memory_numbers_temporary_view')
    return rate_to_write


@pipelines.temporary_view()
def in_memory_numbers_temporary_view():
    sleep_time = random.randint(0, 20)
    print('⏰ Sleeping for', sleep_time, 'seconds')
    time.sleep(sleep_time)
    start_timestamp = int(datetime.datetime.now().timestamp())*1000
    print(f'Start timestamp will be {start_timestamp}')

    spark = SparkSession.active()
    return (spark.readStream.option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .option("startTimestamp", start_timestamp)
      .format("rate-micro-batch").load())