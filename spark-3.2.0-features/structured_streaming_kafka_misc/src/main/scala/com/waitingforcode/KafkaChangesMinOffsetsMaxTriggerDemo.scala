package com.waitingforcode

import org.apache.spark.sql.SparkSession

object KafkaChangesMinOffsetsMaxTriggerDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Kafka changes demo").master("local[*]")
    .getOrCreate()

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "kafka_changes")
    .option("minOffsetsPerTrigger", 5)
    .option("maxTriggerDelay", "1m")
    .load()

  val inputSource = inputKafkaRecords.selectExpr("CAST(value AS STRING)")

  val writeQuery = inputSource
    .writeStream
    .format("console")
    .option("truncate", false)

  inputSource.explain(true)

  writeQuery.start().awaitTermination()

}
