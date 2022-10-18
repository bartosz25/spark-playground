package com.waitingforcode

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession

object SparkListenerExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    sparkSession.sparkContext.addSparkListener(LogPrintingListener)

    import sparkSession.implicits._
    (0 to 100).toDF("nr").repartition(30).collect()
  }

}

object LogPrintingListener extends SparkListener {

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    println(s"Started task with the message: ${taskStart}")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println(s"Ended task with the message: ${taskEnd}")
  }

}