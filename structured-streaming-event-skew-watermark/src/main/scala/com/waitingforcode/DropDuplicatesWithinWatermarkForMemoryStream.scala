package com.waitingforcode

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}

import java.sql.Timestamp
import java.util.TimeZone

object DropDuplicatesWithinWatermarkForMemoryStream {

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    val memoryStream1 = MemoryStream[Event](2)(Encoders.product[Event], sparkSession.sqlContext)

    val query = memoryStream1.toDS
      .withWatermark("eventTime", "20 seconds")
      .dropDuplicatesWithinWatermark("id")

    val writeQuery = query.writeStream.format("console")
      .option("truncate", false).start()

    memoryStream1.addData(
      Seq(
        Event(1, Timestamp.valueOf("2023-06-10 10:20:40")),
        Event(1, Timestamp.valueOf("2023-06-10 10:20:30")),
        Event(2, Timestamp.valueOf("2023-06-10 10:20:50")),
        Event(3, Timestamp.valueOf("2023-06-10 10:20:45")),
      )
    )

    writeQuery.processAllAvailable()
    println(writeQuery.lastProgress.prettyJson)
    writeQuery.explain(true)
    memoryStream1.addData(
      Seq(
        Event(1, Timestamp.valueOf("2023-06-10 10:22:40")),
        Event(1, Timestamp.valueOf("2023-06-10 10:20:10")),
        Event(4, Timestamp.valueOf("2023-06-10 10:21:50")),
        Event(5, Timestamp.valueOf("2023-06-10 10:21:45")),
      )
    )
    writeQuery.processAllAvailable()
    println(writeQuery.lastProgress.prettyJson)

    memoryStream1.addData(
      Seq(
        Event(1, Timestamp.valueOf("2023-06-10 10:24:40")),
      )
    )
    writeQuery.processAllAvailable()
    println(writeQuery.lastProgress.prettyJson)
  }

}

case class Event(id: Int, eventTime: Timestamp)