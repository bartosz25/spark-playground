import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local[*]").config("spark.sql.execution.arrow.maxRecordsPerBatch", 2).getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")
numbers = spark.range(0, 20, numPartitions=5)


@pandas_udf(IntegerType())
def multiply(numbers_vector: pandas.Series) -> pandas.Series:
    print(numbers_vector)
    return numbers_vector.apply(lambda nr: nr * 2)


numbers_with_results = numbers.withColumn("result", multiply(col("id")))

numbers_with_results.show(truncate=False)

numbers_with_results.explain(extended=True)
