package com.waitingforcode

import org.apache.spark.sql.SparkSession

/**
 * To generate some data:
 * ```
 * mkdir /tmp/wfc/structured-streaming-2-sinks/data/ -p
 * echo "$EPOCHSECONDS" > /tmp/wfc/structured-streaming-2-sinks/data/$EPOCHSECONDS.txt
 * ```
 */
object JobWith2Sinks extends App {

  val basePath = "/tmp/wfc/structured-streaming-2-sinks/"

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  import sparkSession.implicits._

  val filesWithNumbers = sparkSession.readStream.text(s"${basePath}/data").as[String].map(_.trim.toInt)

  val multipliedBy2 = filesWithNumbers.map(nr => nr * 2)
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
