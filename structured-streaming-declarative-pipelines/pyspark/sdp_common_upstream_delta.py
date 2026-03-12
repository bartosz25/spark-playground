import datetime
import random
import time

from pyspark import pipelines
from pyspark.sql import SparkSession


@pipelines.table(format='delta')
def rate_sink_1_delta_table():
    spark = SparkSession.active()
    rate_to_write = spark.readStream.table('delta_table_reader')
    return rate_to_write


@pipelines.table(format='delta')
def rate_sink_2_delta_table():
    spark = SparkSession.active()
    rate_to_write = spark.readStream.table('delta_table_reader')
    return rate_to_write


@pipelines.temporary_view()
def delta_table_reader():
    spark = SparkSession.active()
    return spark.readStream.format('delta').option(
        'path', '/opt/spark/data/rate_delta_for_input'
    ).load()