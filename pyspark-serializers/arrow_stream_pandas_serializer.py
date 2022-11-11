import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

# ArrowStreamPandasSerializer
spark = SparkSession.builder.master("local[*]")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

random_pandas_df = pd.DataFrame(np.random.rand(100, 3))

spark_df = spark.createDataFrame(random_pandas_df)

spark_df.show(truncate=False)