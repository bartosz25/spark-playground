package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

import java.io.File

object TriggerAvailableNowApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. Structured Streaming").master("local[*]")
      .getOrCreate()

    val outputTestDir = "/tmp/test_triggers"
    (1 to 6).map(nr => (nr, s"abc${nr}")).foreach {
      case (nr, content) => {
        FileUtils.writeStringToFile(new File(s"${outputTestDir}/file${nr}.txt"), content, "UTF-8")
      }
    }

    val rateMicroBatchSource = sparkSession.readStream
      .option("maxFilesPerTrigger", 2).text(outputTestDir)

    import sparkSession.implicits._
    val consoleSink = rateMicroBatchSource
      .select( $"value", functions.spark_partition_id())
      .writeStream.format("console").trigger(Trigger.AvailableNow())

    consoleSink.start().awaitTermination()
  }

}
