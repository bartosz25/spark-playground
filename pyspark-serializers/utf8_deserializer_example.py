# SparkContext.textFile or wholeTextFiles
from pyspark.sql import SparkSession

file_path = '/tmp/test.txt'
with open(file_path, 'w') as f:
    f.write('line1')
    f.write('line2')
    f.write('line3')

# UTF8Deserializer
# PairDeserializer
spark = SparkSession.builder.master("local[*]")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

spark.sparkContext.wholeTextFiles(file_path, use_unicode=True).collect()