from pyspark import pipelines, Row
from pyspark.sql import SparkSession

## Restarting this flow currently fails because the query runs an `ALTER TABLE`
#  which is not well supported:
# spark-1  | org.apache.spark.sql.pipelines.graph.DatasetManager$TableMaterializationException: org.apache.spark.sql.delta.DeltaAnalysisException: [DELTA_CANNOT_CHANGE_PROVIDER] 'provider' is a reserved table property, and cannot be altered.
# spark-1  | Caused by: org.apache.spark.sql.delta.DeltaAnalysisException: [DELTA_CANNOT_CHANGE_PROVIDER] 'provider' is a reserved table property, and cannot be altered.
pipelines.create_streaming_table('rate_table_append_flow', format='delta')

@pipelines.append_flow(target='rate_table_append_flow')
def rate_source_1():
    spark = SparkSession.active()
    return (spark.readStream.option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .format("rate-micro-batch").load())


@pipelines.append_flow(target='rate_table_append_flow')
def rate_source_2():
    spark = SparkSession.active()
    return (spark.readStream.option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .option("startTimestamp", 1769968633000)
      .format("rate-micro-batch").load())
