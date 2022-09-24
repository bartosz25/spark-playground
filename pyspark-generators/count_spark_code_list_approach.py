import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
    .appName("Yield in mapPartitions")\
    .getOrCreate()

import resource
def using(point=""):
    usage=resource.getrusage(resource.RUSAGE_SELF)
    return '''%s: usertime=%s systime=%s mem=%s mb
           '''%(point,usage[0],usage[1],
                usage[2]/1024.0 )

input_data = spark.sparkContext.parallelize(list(range(0, 10000000)), 1)

def map_numbers_with_list(numbers):
    output_letters = []
    for number in numbers:
        output_letters.append(number*200)
        if number == 0 or number >= 10000000 - 2:
            print(using(f"list{number}"))
    return output_letters


mapped_result = input_data.mapPartitions(map_numbers_with_list).count()