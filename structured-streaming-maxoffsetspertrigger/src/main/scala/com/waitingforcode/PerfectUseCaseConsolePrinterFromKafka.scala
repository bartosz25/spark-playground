package com.waitingforcode

import org.apache.spark.sql.SparkSession

/*
start producer
start consumer
... produce some data
rm -rf /tmp/waitingforcode/less_perfect_use_case/
replay the consumer
rm -rf /tmp/waitingforcode/perfect_use_case/
replay the consumer

<==
 */
object PerfectUseCaseConsolePrinterFromKafka extends App {

  val sparkSession = SparkSession.builder()
    .appName("Console printer from kafka").master("local[*]")
    .getOrCreate()

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", PerfectUseCaseTopicName)
    .option("startingOffsets", "EARLIEST")
    .option("maxOffsetsPerTrigger", 4)
    .load()
    .selectExpr("CAST(value AS STRING)")

  val writeQuery = inputKafkaRecords
    .writeStream
    .option("checkpointLocation", PerfectUseCaseCheckpoint)
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()
}
