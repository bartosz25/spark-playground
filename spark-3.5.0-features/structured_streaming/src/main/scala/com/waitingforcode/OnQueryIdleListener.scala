package com.waitingforcode

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryIdleEvent
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.TimeUnit


object OnQueryIdleListener {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()

    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryIdle(event: QueryIdleEvent): Unit = {
        println("Query is idle:")
        println(event.json)
      }
    })

    import sparkSession.implicits._
    implicit val sparkContext: SQLContext = sparkSession.sqlContext
    val memoryStream1 = MemoryStream[Int]

    val query = memoryStream1.toDS

    val writeQuery = query.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .option("truncate", false).start()

    memoryStream1.addData(Seq(1, 2, 3))
    writeQuery.awaitTermination()
  }

}
