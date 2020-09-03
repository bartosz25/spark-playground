package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}

object FailedStreamingLeftOuterJoin245 extends App {

  val session = SparkSession.builder()
    .appName("Failed streaming left outer join").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  val rateStream = session.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
  
  val leftStream = rateStream
    .select(functions.col("value").as("leftId"), 
      functions.col("timestamp").as("leftTime"))

  val rightStream = rateStream
    .select(functions.col("value").as("rightId"),
      functions.col("timestamp").as("rightTime"))


  val query = leftStream
    .withWatermark("leftTime", "5 seconds")
    // Because of the error in Spark 2.4.5, the matched rows should be returned
    // twice. First, when they have matches and later when the left side expires.
    // If you run the same code in spark-3.0-0 package, you'll see that it
    // doesn't happen in the Spark 3 release.
    .join(
      rightStream.withWatermark("rightTime", "10 seconds"),
      functions.expr(
        "rightId = leftId AND rightTime >= leftTime AND rightTime <= leftTime + interval 5 seconds"
      ),
      joinType = "leftOuter"
    )
    .writeStream
    .format("console")
    .option("truncate", false)
    .start()

  query.awaitTermination()

}
