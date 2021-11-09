package com.waitingforcode

import org.apache.spark.sql.SparkSession

object KafkaChangesStartingTimestampDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Kafka changes demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2) // 2 physical state stores; one per shuffle partition task
    .getOrCreate()

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "kafka_changes")
    .option("startingTimestamp", 1636028305000L)
    .load()

  val inputSource = inputKafkaRecords.selectExpr("CAST(value AS STRING)")

  val writeQuery = inputSource
    .writeStream
    .format("console")
    .option("truncate", false)

  inputSource.explain(true)

  writeQuery.start().awaitTermination()

}
