package com.waitingforcode

import org.apache.spark.sql.{Row, SQLContext, SparkSession, functions}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, OutputMode}

import java.sql.Timestamp

object CompleteModeForAggregatesDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    implicit val sparkContext: SQLContext = sparkSession.sqlContext
    import sparkSession.implicits._
    val events = MemoryStream[TimestampedEvent]

    events.addData(
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:00:00")),
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:02:00")),
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:04:00")),
      TimestampedEvent(2, Timestamp.valueOf("2024-04-01 09:02:50"))
    )

    val query = events.toDF().groupBy($"eventId").count().writeStream.outputMode(OutputMode.Complete())
      .format("console").option("truncate", false).start()

    query.explain(true)
    query.processAllAvailable()
    events.addData(
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:00:00")),
      TimestampedEvent(2, Timestamp.valueOf("2024-04-01 09:02:50"))
    )
    query.processAllAvailable()


    events.addData(
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:00:00")),
    )
    query.processAllAvailable()
  }

}