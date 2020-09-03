package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ApiBasedProcessing extends App {

  val spark = SparkSession.builder()
    .appName("API-based processing").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()


  val inputData = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", SyntaxCheckConfiguration.TopicName)
    .option("startingOffsets", "EARLIEST")
    .load()

  val usedProcessingQuery = inputData.select("value", "timestamp")

  val streamingQuery = usedProcessingQuery.writeStream.format("console").option("truncate", false)
    .start()

  new Thread(() => {
    while (!streamingQuery.isActive) {}
    Thread.sleep(5000L)
    streamingQuery.explain(true)
  }).start()

  streamingQuery.awaitTermination()

}
