import datetime
import random
import time

from pyspark import pipelines
from pyspark.sql import SparkSession


@pipelines.table(format='delta')
def rate_sink_1():
    spark = SparkSession.active()
    rate_to_write = spark.readStream.table('kafka_rate_topic_reader')
    return rate_to_write


@pipelines.table(format='delta')
def rate_sink_2():
    spark = SparkSession.active()
    rate_to_write = spark.readStream.table('kafka_rate_topic_reader')
    return rate_to_write


@pipelines.temporary_view()
def kafka_rate_topic_reader():
    sleep_time = random.randint(60, 90)
    print('⏰ Sleeping for', sleep_time, 'seconds')
    #time.sleep(sleep_time)
    print(f'Start timestamp will be {datetime.datetime.now()}')

    spark = SparkSession.active()
    return (spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', 'kafka:9092')
            .option('subscribe', 'rate-data')
            .option('startingOffsets', 'earliest')
            .load())