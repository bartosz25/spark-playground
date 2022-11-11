import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType

# ArrowStreamSerializer
# ArrowStreamPandasSerializer
# ArrowStreamPandasUDFSerializer
spark = SparkSession.builder.master("local[*]")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

random_pandas_df = pd.DataFrame(np.random.rand(100, 3))

spark_df = spark.createDataFrame(random_pandas_df)

@pandas_udf('double', PandasUDFType.SCALAR)
def multiply(nr):
    return nr * 2

df = spark.createDataFrame([(1, "a"), (2, "b"),  (3, "c"),  (4, "d")], ("nr", "letter"))

df.withColumn('multiplication_result', multiply(df.nr)).show(truncate=False)
