package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.TimestampType

import java.io.File

object CorrectnessIssueJoinStillPresent {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/spark/3.4.0/correctness_issue_present/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import sparkSession.implicits._
    implicit val sparkContext = sparkSession.sqlContext
    val memoryStream1 = MemoryStream[Int]
    val memoryStream2 = MemoryStream[Int]
    val memoryStream3 = MemoryStream[Int]

    val data1 = memoryStream1.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")
    val data2 = memoryStream2.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")
    val data3 = memoryStream3.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")

    val join1 = data1.join(data2, Seq("value", "event_time"), "leftOuter")

    val join2 = join1.join(data3, Seq("value", "event_time"), "leftOuter")

    val joinToWrite = join2

    memoryStream1.addData(Seq(1, 2, 3))
    memoryStream2.addData(Seq(30))
    memoryStream3.addData(Seq(4, 5))
    val query = joinToWrite.writeStream.format("console")
      .option("checkpointLocation", checkpointLocation).start()
    query.processAllAvailable()
    //query.explain(true)
    //println(query.lastProgress.prettyJson)

    memoryStream1.addData(Seq(6))
    memoryStream2.addData(Seq(6))
    memoryStream3.addData(Seq(6))
    query.processAllAvailable()
    //query.explain(true)
    //println(query.lastProgress.prettyJson)

    query.awaitTermination()
  }


}
