package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaConsumer300 extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("Consumer 3.0.0").master("local[*]")
    .config("spark.ui.port", 3000)
    .getOrCreate()

  val dataFrame = testSparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "test_topic")
    .option("startingOffsets", "EARLIEST")
    .load()

  dataFrame.writeStream.trigger(Trigger.ProcessingTime(2000L))
    .format("console").start().awaitTermination()


}
