from pyspark import pipelines, Row
from pyspark.sql import SparkSession, DataFrame, functions as F


def add_processing_time(dataframe_to_decorate: DataFrame) -> DataFrame:
    return (dataframe_to_decorate
            .withColumn("processing_time", F.current_timestamp()))

@pipelines.table(format='delta')
def error_rate_data_with_processing_time():
    spark = SparkSession.active()
    rate_data = (spark.readStream.option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .format("rate-micro-batch").load())

    new_letters = spark.createDataFrame(
        data=[
            Row(value=1, processing_time='A', timestamp='a')
        ]
    )

    rate_data_with_processing_time_df = add_processing_time(rate_data)
    return rate_data_with_processing_time_df.unionByName(new_letters)
