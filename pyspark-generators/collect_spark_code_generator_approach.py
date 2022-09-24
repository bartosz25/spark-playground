import logging
import sys  # Put at top if not already there
import time
from venv import logger

from pyspark.sql import SparkSession

sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.DEBUG)
logger.addHandler(sh)

logger = logging.getLogger("py4j")
## Enable to see the communication between Py4j Python and Py4j JVM
#logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

spark = SparkSession.builder.master("local[1]")\
    .appName("Yield in mapPartitions")\
    .getOrCreate()


input_data = spark.sparkContext.parallelize(list(range(0, 10)), 1)

def map_numbers_with_generator(numbers):
    for number in numbers:
        print('yield')
        yield number*200

mapped_result = input_data.mapPartitions(map_numbers_with_generator).collect()
