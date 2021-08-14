package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream

object RetriesMicroBatchTriggerDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Retries demo for micro-batch trigger")
    .master("local[3, 6]") // # threads, # retries
    .getOrCreate()
  import sparkSession.implicits._

  val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
  inputStream.addData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

  inputStream.toDS().toDF("number")
    .as[Int]
    .map(nr => {
      if (nr == 3) {
        throw new RuntimeException("nr 3 detected!")
      }
      nr
    })
    .writeStream.format("console").start().awaitTermination()
}
