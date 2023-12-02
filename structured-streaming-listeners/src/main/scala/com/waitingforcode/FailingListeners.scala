package com.waitingforcode

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.{SQLContext, SparkSession}


object FailingListeners {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()

    sparkSession.streams.addListener(new DummyListener("A"))
    sparkSession.streams.addListener(new ProgressFailingListener())
    sparkSession.streams.addListener(new DummyListener("B"))

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

class DummyListener(val code: String) extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    println(s"Progress ${code}")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

class ProgressFailingListener extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    throw new RuntimeException("Failure!")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}