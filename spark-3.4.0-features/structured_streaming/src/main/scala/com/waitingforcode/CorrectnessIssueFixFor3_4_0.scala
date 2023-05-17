package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.TimestampType

import java.io.File

object CorrectnessIssueFixFor3_4_0 {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/spark/3.4.0/correctness_issue_fix/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true")
      .config("spark.sql.streaming.unsupportedOperationCheck", "true")
      // You can turn this flag off/on if you want to see different behavior without/with the
      // same/different watermarks for eviction and late data
      .config("spark.sql.streaming.statefulOperator.allowMultiple", true)
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import sparkSession.implicits._
    implicit val sparkContext = sparkSession.sqlContext
    val memoryStream1 = MemoryStream[Int]

    val query = memoryStream1.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")
      .groupBy(functions.window($"event_time", "2 seconds").as("first_window"))
      .count()
      .groupBy(functions.window($"first_window", "5 seconds").as("second_window"))
      .agg(functions.count("*"), functions.sum("count").as("sum_of_counts"))

    val writeQuery = query.writeStream.format("console").option("truncate", false).start()


    memoryStream1.addData(Seq(1, 2, 3))
    writeQuery.processAllAvailable()
    //query.explain(true)

    memoryStream1.addData(Seq(6))
    writeQuery.processAllAvailable()
    query.explain(true)

    memoryStream1.addData(Seq(14))
    writeQuery.processAllAvailable()
    memoryStream1.addData(Seq(14))
    writeQuery.processAllAvailable()
    writeQuery.awaitTermination()
  }

}
