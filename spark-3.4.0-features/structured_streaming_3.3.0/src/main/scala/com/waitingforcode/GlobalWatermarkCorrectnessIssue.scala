package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.TimestampType

object GlobalWatermarkCorrectnessIssue {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import sparkSession.implicits._
    implicit val sparkContext = sparkSession.sqlContext
    val memoryStream1 = MemoryStream[Int]
    val memoryStream2 = MemoryStream[Int]

    val data1 = memoryStream1.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")
    val data2 = memoryStream2.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")

    val join1 = data1.join(data2, Seq("value", "event_time"), "leftOuter")
      .groupBy($"event_time").count()

    memoryStream1.addData(Seq(1, 2, 3))
    memoryStream2.addData(Seq(30))
    val query = join1.writeStream.format("console").start()
    query.processAllAvailable()
    //query.explain(true)
    //println(query.lastProgress.prettyJson)

    memoryStream1.addData(Seq(6))
    memoryStream2.addData(Seq(6))
    query.processAllAvailable()
    //query.explain(true)
    //println(query.lastProgress.prettyJson)

    while (true) {}
  }


}
