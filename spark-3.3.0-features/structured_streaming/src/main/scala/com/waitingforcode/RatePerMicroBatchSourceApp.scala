package com.waitingforcode

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

object RatePerMicroBatchSourceApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. Structured Streaming").master("local[*]")
      .getOrCreate()

    val rateMicroBatchSource = sparkSession.readStream
      .option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .option("startTimestamp",
        LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .format("rate-micro-batch").load()

    import sparkSession.implicits._
    val consoleSink = rateMicroBatchSource
      .select($"timestamp", $"value", functions.spark_partition_id())
      .writeStream.format("console").trigger(Trigger.ProcessingTime("5 seconds"))

    consoleSink.start().awaitTermination()
  }

}
