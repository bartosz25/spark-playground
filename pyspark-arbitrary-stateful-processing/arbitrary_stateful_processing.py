from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, TimestampType, BooleanType, StringType

from stateful_mapper import map_with_state

spark_session = SparkSession.builder.master("local[*]") \
    .config("spark.sql.shuffle.partitions", 2).getOrCreate()

input_data = spark_session.readStream.format("rate-micro-batch") \
    .options(rowsPerBatch=50, numPartitions=2, advanceMillisPerBatch=3000).load()

grouped_visits = input_data\
    .selectExpr('value', 'timestamp', 'value % 6 AS group_id')\
    .withWatermark('timestamp', '15 minutes') \
    .groupBy(F.col('group_id'))


visits = grouped_visits.applyInPandasWithState(
    func=map_with_state,
    outputStructType=StructType([
        StructField("group_id", IntegerType()),
        StructField("start_time", StringType()),
        StructField("end_time", StringType()),
        StructField("duration_in_milliseconds", LongType()),
        StructField("is_final", BooleanType())
    ]),
    stateStructType=StructType([
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType())
    ]),
    outputMode="update",
    timeoutConf="EventTimeTimeout"
)

write_query = visits.writeStream.outputMode("update") \
    .option("checkpointLocation", f"/tmp/wfc/pyspark/arbitrary/{datetime.utcnow().isoformat()}")\
    .format("console").option("truncate", False)\
    .start()

write_query.awaitTermination()
