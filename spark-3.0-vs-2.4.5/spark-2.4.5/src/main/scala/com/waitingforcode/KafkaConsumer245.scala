package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaConsumer245 extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("Consumer 2.4.5").master("local[*]")
    .config("spark.ui.port", 2450)
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
