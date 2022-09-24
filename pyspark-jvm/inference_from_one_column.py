import logging
import os
import time
from typing import List

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
    .appName("One column inference")\
    .getOrCreate()


letters = [{'letter': 'a'}, {'letter': 'b'}, {'letter': 'c'}]
#spark.createDataFrame(letters).show(truncate=False)
#spark.createDataFrame(letters).filter("a")


#spark.sparkContext.setLogLevel("TRACE")
import sys # Put at top if not already there
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.DEBUG)
logger = logging.getLogger('py4j')
logger.addHandler(sh)

file_content = """a
b
c
d
f"""
file_path = "/tmp/test_data.txt"
output_file = open(file_path, 'w')
output_file.write(file_content)
output_file.close()


text_file_rdd = spark.sparkContext.textFile(file_path, 2)
print(type(text_file_rdd))

def map_letter(letter: str) -> str:
    print(f'Mapping a {letter}')
    return f'{letter*2}x'

def printForeach(letters: List[str]):
    time.sleep(50)
    for l in letters:
        print(f'Got {l}')

mapped_text_lines = text_file_rdd.map(map_letter)
print(type(mapped_text_lines))


mapped_text_lines.saveAsTextFile('/tmp/test_data_output')
