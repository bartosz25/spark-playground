package com.waitingforcode

import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{SparkSession, functions}

object StreamingQueryListenerExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    sparkSession.streams.addListener(StreamingQueryPrintingListener)

    val rateMicroBatchSource = sparkSession.readStream
      .option("rowsPerSecond", 5)
      .option("numPartitions", 2)
      .format("rate").load()

    import sparkSession.implicits._
    val consoleSink = rateMicroBatchSource
      .select($"timestamp", $"value", functions.spark_partition_id())
      .writeStream.format("console").trigger(Trigger.ProcessingTime("2 seconds"))

    consoleSink.start().awaitTermination()
  }

}

object StreamingQueryPrintingListener extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s">>> Query started ${event}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    println(s">>> Query made progress ${event}")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s">>> Query terminated ${event}")
  }
}