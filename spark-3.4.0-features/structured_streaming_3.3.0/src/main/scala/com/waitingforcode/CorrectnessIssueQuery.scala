package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.TimestampType

import java.io.File

object CorrectnessIssueQuery {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/spark/3.4.0/correctness_issue/checkpoint"
    FileUtils.deleteDirectory(new File(checkpointLocation))
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true")
      .config("spark.sql.streaming.unsupportedOperationCheck", "true")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import sparkSession.implicits._
    implicit val sparkContext = sparkSession.sqlContext
    val memoryStream1 = MemoryStream[Int]

    val query = memoryStream1.toDF.withColumn("event_time", $"value".cast(TimestampType))
      .withWatermark("event_time", "0 seconds")
      .groupBy(functions.window($"event_time", "2 seconds").as("first_window"))
      .count()
      .groupBy(functions.window($"first_window", "10 seconds").as("second_window"))
      .agg(functions.count("*"), functions.sum("count").as("sum_of_counts"))

    val writeQuery = query.writeStream.format("console").option("truncate", false).start()


    memoryStream1.addData(Seq(1000, 2000, 3000))
    writeQuery.processAllAvailable()
    //query.explain(true)

    memoryStream1.addData(Seq(6000))
    writeQuery.processAllAvailable()
    //query.explain(true)7

    memoryStream1.addData(Seq(14000))
    writeQuery.awaitTermination()
  }

}
