package com.waitingforcode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object AutoscaledRateStreamReader extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#dynamic-resource-allocation-structured-streaming")
      .getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("rate")
      .option("numPartitions", 10)
      .option("rowsPerSecond", 20)
      .load()

    val mappedStream = inputStream.as[(Timestamp, Long)].map {
      case (_, nr) => {
        if (nr % 9 == 0) {
          logInfo(s"Sleeping for 1 minute because of the number ${nr}")
          Thread.sleep(60000L)
        }
        nr
      }
    }

    mappedStream.writeStream.format("console").start().awaitTermination()
  }
}