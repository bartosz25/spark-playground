package com.waitingforcode

import org.apache.spark.sql.SparkSession


/*
start producer
start consumer
... produce some data ==> stop at the 3rd micro-batch
rm /tmp/waitingforcode/less_perfect_use_case/checkpoint/commits/3
rm /tmp/waitingforcode/less_perfect_use_case/checkpoint/offsets/3
replay the consumer

<==
 */
object LessPerfectUseCaseConsolePrinterFromKafka extends App {

  val sparkSession = SparkSession.builder()
    .appName("Console printer from kafka").master("local[*]")
    .getOrCreate()

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", LessPerfectUseCaseTopicName)
    .option("startingOffsets", "EARLIEST")
    .option("maxOffsetsPerTrigger", 4)
    .load()
    .selectExpr("CAST(value AS STRING)")

  val writeQuery = inputKafkaRecords
    .writeStream
    .option("checkpointLocation", LessPerfectUseCaseCheckpoint)
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()
}
