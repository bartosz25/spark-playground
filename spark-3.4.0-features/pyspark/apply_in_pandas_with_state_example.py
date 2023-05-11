from typing import Iterator, Any

import pandas
from pyspark.sql import SparkSession, functions
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark_session = SparkSession.builder.master("local[*]").getOrCreate()

input_data = spark_session.readStream.format("rate-micro-batch") \
    .options(rowsPerBatch=5, numPartitions=1, advanceMillisPerBatch=3000).load()

input_data_enriched = input_data.withColumn("even_odd_label", functions.when(
    functions.pmod(functions.col("value"), 2) == 0, "even").otherwise("odd"))


def count_labeled_numbers(key: Any, input_rows: Iterator[pandas.DataFrame], state: GroupState) -> Iterator[pandas.DataFrame]:
    session_state = "active"
    if state.hasTimedOut:
        count = state.get
        session_state = "timed_out"
        state.remove()
    else:
        count = 0
        for inp in input_rows:
            count += len(inp.index)
        if state.exists:
            old_count = state.get
            count += old_count[0]
        print(f'Updating state {state}')
        state.update((count,))
        state.setTimeoutDuration(1000)
    yield pandas.DataFrame({
        "label": key,
        "count": count,
        "state": session_state
    })


even_odd_counts_from_stateful = input_data_enriched.withWatermark("timestamp", "5 seconds") \
    .groupBy("even_odd_label") \
    .applyInPandasWithState(
    func=count_labeled_numbers,
    outputStructType=StructType([
        StructField("label", StringType()),
        StructField("count", IntegerType()),
        StructField("state", StringType())
    ]),
    stateStructType=StructType([StructField("count", IntegerType())]),
    outputMode="update",
    timeoutConf="ProcessingTimeTimeout"
)

write_query = even_odd_counts_from_stateful.writeStream.outputMode("update").format("console").option("truncate",
                                                                                                      False).start()

write_query.awaitTermination()
