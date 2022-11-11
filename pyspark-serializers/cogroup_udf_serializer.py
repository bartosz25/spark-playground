import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

# ArrowStreamPandasSerializer
# ArrowStreamPandasUDFSerializer
spark = SparkSession.builder.master("local[*]")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))


def mean_func(key, pdf):
    return pd.DataFrame([key + (pdf.v.mean(),)])

df.groupby('id').applyInPandas(mean_func, schema = "id long, v double").show()
