import pyarrow
from pyspark.sql import SparkSession

# ArrowStreamSerializer
# ArrowStreamUDFSerializer
spark = SparkSession.builder.master("local[1]")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b"),  (3, "c"),  (4, "d")], ("nr", "letter"))


def keep_even_numbers_filter(iterator):
    for batch in iterator:
        pandas_df = batch.to_pandas()
        yield pyarrow.RecordBatch.from_pandas(pandas_df[pandas_df.nr % 2 == 0])


df.mapInArrow(keep_even_numbers_filter, df.schema).show()
