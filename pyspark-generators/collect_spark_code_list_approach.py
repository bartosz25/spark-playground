import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
    .appName("Yield in mapPartitions")\
    .getOrCreate()

input_data = spark.sparkContext.parallelize(list(range(0, 10)), 1)

def map_numbers_with_list(numbers):
    output_letters = []
    for number in numbers:
        print('append')
        output_letters.append(number*200)
    end_time = int(round(time.time() * 1000))
    return output_letters


mapped_result = input_data.mapPartitions(map_numbers_with_list).collect()