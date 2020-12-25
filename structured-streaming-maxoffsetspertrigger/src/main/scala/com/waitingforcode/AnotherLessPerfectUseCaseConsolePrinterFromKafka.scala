package com.waitingforcode

import org.apache.spark.sql.SparkSession


/*
start producer
start consumer
... produce some data
rm /tmp/waitingforcode/another_less_perfect_use_case/checkpoint
replay the consumer
<==
 */
object AnotherLessPerfectUseCaseConsolePrinterFromKafka extends App {

  val sparkSession = SparkSession.builder()
    .appName("Console printer from kafka").master("local[*]")
    .getOrCreate()

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", AnotherLessPerfectUseCaseTopicName)
    .option("startingOffsets", "EARLIEST")
    .option("maxOffsetsPerTrigger", 4)
    .load()
    .selectExpr("CAST(value AS STRING)")

  val writeQuery = inputKafkaRecords
    .writeStream
    .option("checkpointLocation", AnotherLessPerfectUseCaseCheckpoint)
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()
}
