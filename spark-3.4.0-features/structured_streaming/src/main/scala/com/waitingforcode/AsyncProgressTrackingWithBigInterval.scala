package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SparkSession, functions}

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

object AsyncProgressTrackingWithBigInterval {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/spark/3.4.0/async_progress_tracking/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))
    val sparkSession = SparkSession.builder().master("local[*]")
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
      .writeStream.format("console")
      .option("checkpointLocation", checkpointLocation)
      .option("asyncProgressTrackingEnabled", true)
      .option("asyncProgressTrackingCheckpointIntervalMs", TimeUnit.SECONDS.toMillis(5))

    consoleSink.start().awaitTermination()
  }

}
