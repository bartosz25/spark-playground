package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.sql.Timestamp

object RetriesContinuousTriggerDemo extends App {


  val sparkSession = SparkSession.builder()
    .appName("Retries demo for continuous trigger").master("local[3, 6]") // # threads, # retries
    .getOrCreate()
  import sparkSession.implicits._

    sparkSession.readStream.format("rate").option("rowsPerSecond", "10")
    .option("numPartitions", "2")
      .load().as[(Timestamp, Long)]
      .map(nr => {
        if (nr._2 == 3) {
          throw new RuntimeException("nr 3 detected!")
        }
        nr._2
      }).writeStream.trigger(Trigger.Continuous("2 second")).format("console").start().awaitTermination()
}
