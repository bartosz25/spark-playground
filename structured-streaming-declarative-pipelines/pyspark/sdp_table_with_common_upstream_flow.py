from pyspark import pipelines, Row
from pyspark.sql import SparkSession

## Restarting this flow currently fails because the query runs an `ALTER TABLE`
#  which is not well supported:
# spark-1  | org.apache.spark.sql.pipelines.graph.DatasetManager$TableMaterializationException: org.apache.spark.sql.delta.DeltaAnalysisException: [DELTA_CANNOT_CHANGE_PROVIDER] 'provider' is a reserved table property, and cannot be altered.
# spark-1  | Caused by: org.apache.spark.sql.delta.DeltaAnalysisException: [DELTA_CANNOT_CHANGE_PROVIDER] 'provider' is a reserved table property, and cannot be altered.
pipelines.create_streaming_table('common_upstream_flow_1', format='delta')
pipelines.create_streaming_table('common_upstream_flow_2', format='delta')

@pipelines.append_flow(target='common_upstream_flow_1')
@pipelines.append_flow(target='common_upstream_flow_2')
def rate_source_for_two_flows():
    spark = SparkSession.active()
    return (spark.readStream.option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .format("rate-micro-batch").load())
