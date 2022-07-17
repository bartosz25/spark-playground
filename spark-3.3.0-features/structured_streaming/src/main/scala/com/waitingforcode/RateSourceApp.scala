package com.waitingforcode

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

object RateSourceApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. Structured Streaming").master("local[*]")
      .getOrCreate()

    val rateMicroBatchSource = sparkSession.readStream
      .option("rowsPerSecond", 5)
      .option("numPartitions", 2)
      .format("rate").load()

    import sparkSession.implicits._
    val consoleSink = rateMicroBatchSource
      .select($"timestamp", $"value", functions.spark_partition_id())
      .writeStream.format("console").trigger(Trigger.ProcessingTime("5 seconds"))

    consoleSink.start().awaitTermination()
  }

}
