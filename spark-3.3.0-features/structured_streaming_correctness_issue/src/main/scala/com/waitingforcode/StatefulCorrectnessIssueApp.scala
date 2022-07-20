package com.waitingforcode

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.{Row, SparkSession, functions}

import java.util.Date

object StatefulCorrectnessIssueApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. Structured Streaming").master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()

    val rateMicroBatchSource = sparkSession.readStream
      .option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .format("rate").load()

    import sparkSession.implicits._
    val consoleSink = rateMicroBatchSource
      .select($"timestamp", $"value".mod(2).as("modulo_rest"), functions.spark_partition_id())
      .withWatermark("timestamp", "4 seconds")
      .groupByKey(row => row.getAs[Long]("modulo_rest"))
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(StatefulMapper.mapDataStatefully _)
      .writeStream.format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("5 seconds")).outputMode("update")

    consoleSink.start().awaitTermination()
  }

}

object StatefulMapper {

  def mapDataStatefully(key: Long, data: Iterator[Row], state: GroupState[String]): Option[String] = {
    println(s"wm=${state.getCurrentWatermarkMs()}")
    if (!state.exists || !state.hasTimedOut) {
      state.update(s"state for ${key} from ${new Date()}")
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs()+4000L)
    } else if (state.hasTimedOut) {
      println(s"State ${state.get} has timed out!")
      state.remove()
    }
    state.getOption
  }

}
