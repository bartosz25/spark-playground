from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local[*]").getOrCreate()

numbers = spark.range(0, 20, numPartitions=5)


@udf(IntegerType())
def multiply(number: int) -> int:
    print(number)
    return number * 2


numbers_with_results = numbers.withColumn("result", multiply(col("id")))

numbers_with_results.show(truncate=False)