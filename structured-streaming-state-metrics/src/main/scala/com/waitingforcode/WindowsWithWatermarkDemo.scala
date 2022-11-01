package com.waitingforcode

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions}

/**
  * Setup
  * docker exec -ti broker_kafka_1 bin/bash
  * kafka-topics.sh --bootstrap-server localhost:29092 --topic windows_watermark --delete
  * kafka-topics.sh --bootstrap-server localhost:29092 --topic windows_watermark --create
  * kafka-console-producer.sh --bootstrap-server localhost:29092 --topic windows_watermark
  *
  *
  */
/*
# Demo scenario
1. Explain on the whiteboard
1. Show numUpdatedRows and numTotalRows inside the StateStoreSaveExec
{"event_time": "2020-05-05T01:20:00", "count": 5}
{"event_time": "2020-05-05T01:26:00", "count": 3}

2. Move the watermark to show the number of removed rows
{"event_time": "2020-05-05T02:15:00", "count": 2}

3.

*/
object WindowsWithWatermarkDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Windows with watermark demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("client.id", "windows_watermark_client")
    .option("subscribe", "windows_watermark")
    .option("startingOffsets", "EARLIEST")
    .load()

  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("count", IntegerType)
  ))

  val window = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "25 minutes")
    .groupBy(functions.window($"event_time", "10 minutes"))

  val writeQuery = window.sum("count")
    .writeStream
    .option("checkpointLocation", s"/tmp/wfc/window-watermark${System.currentTimeMillis()}")
    .outputMode(OutputMode.Update)
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()

}
