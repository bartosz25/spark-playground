package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object ContinuousModeWithQueueSize extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Continuous mode with queue size").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.sql.streaming.continuous.epochBacklogQueueSize", 5)
    .getOrCreate()

  val rateStream = sparkSession.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 2)
    .load()

  import sparkSession.implicits._
  val writeQuery = rateStream
    .select("value").as[Long]
    .map(number => {
      if (number % 2 == 0) {
        // With this sleep I'm simulating a slower processing
        // on one of the partitions
        // It will accumulate the pending (still-in-progress) partitions
        // and therefore, make the query fail due to the
        // epochBacklogQueueSize reached
        //Thread.sleep(4000)
      }
      number
    })
    .writeStream
    .trigger(Trigger.Continuous("2 seconds"))
    .option("truncate", false)
    .format("console")

  writeQuery.start().awaitTermination()
}
