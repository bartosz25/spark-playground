import functools
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, TimestampType, BooleanType, StringType

from output_handlers import OutputHandler
from state_handlers import StructFieldWithStateUpdateHandler, StateSchemaHandler
from stateful_mapper_refactored import map_with_state_refactored

spark_session = SparkSession.builder.master("local[*]") \
    .config("spark.sql.shuffle.partitions", 2).getOrCreate()

input_data = spark_session.readStream.format("rate-micro-batch") \
    .options(rowsPerBatch=50, numPartitions=2, advanceMillisPerBatch=3000).load()

grouped_visits = input_data \
    .selectExpr('value', 'timestamp', 'value % 6 AS group_id') \
    .withWatermark('timestamp', '15 minutes') \
    .groupBy(F.col('group_id'))

state_handler = StateSchemaHandler(
    start_time=StructFieldWithStateUpdateHandler(StructField("start_time", TimestampType())),
    end_time=StructFieldWithStateUpdateHandler(StructField("end_time", TimestampType()))
)

output_handler = OutputHandler(
    group_id=StructField("group_id", IntegerType()),
    start_time=StructField("start_time", StringType()),
    end_time=StructField("end_time", StringType()),
    duration_in_milliseconds=StructField("duration_in_milliseconds", LongType()),
    is_final=StructField("is_final", BooleanType()),
    state_schema_handler=state_handler
)

visits = grouped_visits.applyInPandasWithState(
    func=functools.partial(map_with_state_refactored, state_handler, output_handler),
    outputStructType=output_handler.schema,
    stateStructType=state_handler.schema,
    outputMode="update",
    timeoutConf="EventTimeTimeout"
)

write_query = visits.writeStream.outputMode("update") \
    .option("checkpointLocation", f"/tmp/wfc/pyspark/arbitrary/{datetime.utcnow().isoformat()}") \
    .format("console").option("truncate", False) \
    .start()

write_query.awaitTermination()
