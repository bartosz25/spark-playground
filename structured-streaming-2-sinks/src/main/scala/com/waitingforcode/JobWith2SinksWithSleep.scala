package com.waitingforcode

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * To generate some data:
 * ```
 * mkdir /tmp/wfc/structured-streaming-2-sinks-with-sleep/data/ -p
 * echo "$EPOCHSECONDS" > /tmp/wfc/structured-streaming-2-sinks-with-sleep/data/$EPOCHSECONDS.txt
 * ```
 */
object JobWith2SinksWithSleep extends App {

  val basePath = "/tmp/wfc/structured-streaming-2-sinks-with-sleep/"

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()

  import sparkSession.implicits._

  val filesWithNumbers = sparkSession
    .readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .load.select("value").as[Long].map(nrAsString => {
      Thread.sleep(Random.nextInt(3000))
      println(s"Converting $nrAsString")
      nrAsString.toInt
  })



  val multipliedBy2 = filesWithNumbers.map(nr => nr * 3)
  val multipliedBy2Writer = multipliedBy2.writeStream.format("json")
    .option("path", s"${basePath}/output/sink-1")
    .option("checkpointLocation", s"${basePath}/checkpoint/sink-1")
    .start()

  val multipliedBy3 = filesWithNumbers.map(nr => nr * 3)
  val multipliedBy3Writer = multipliedBy3.writeStream.format("json")
    .option("path", s"${basePath}/output/sink-2")
    .option("checkpointLocation", s"${basePath}/checkpoint/sink-2")
    .start()

  sparkSession.streams.awaitAnyTermination()
}
