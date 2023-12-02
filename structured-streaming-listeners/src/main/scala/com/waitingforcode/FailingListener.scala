package com.waitingforcode

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryIdleEvent
import org.apache.spark.sql.{SQLContext, SparkSession}


object FailingListener {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()

    var alreadyFailed = false
    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("Failure")
        throw new RuntimeException("Error on query started")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        if (!alreadyFailed) {
          alreadyFailed = true
          println("Failure on progress")
          throw new RuntimeException("Failed on progress")
        }
        println("Query progress")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("Query terminated")
      }

      override def onQueryIdle(event: QueryIdleEvent): Unit = {
        println("Query is idle")
        println(event.json)
      }
    })

    import sparkSession.implicits._
    implicit val sparkContext: SQLContext = sparkSession.sqlContext
    val memoryStream1 = MemoryStream[Int]

    val query = memoryStream1.toDS

    val writeQuery = query.writeStream
      .format("console")
      .option("truncate", false).start()

    memoryStream1.addData(Seq(1, 2, 3))
    writeQuery.processAllAvailable()

    memoryStream1.addData(Seq(4, 5, 6))
    writeQuery.processAllAvailable()
    writeQuery.processAllAvailable()
  }

}
