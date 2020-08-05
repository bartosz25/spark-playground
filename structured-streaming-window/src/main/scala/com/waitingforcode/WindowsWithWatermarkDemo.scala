package com.waitingforcode

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions}

/**
  * Setup
  * docker exec -ti kafka_kafka_1 bin/bash
  * kafka-topics.sh --bootstrap-server localhost:29092 --topic windows_watermark --delete
  * kafka-topics.sh --bootstrap-server localhost:29092 --topic windows_watermark --create
  * kafka-console-producer.sh --bootstrap-server localhost:29092 --topic windows_watermark
  *
  *
  */
/*
{"event_time": "2020-05-05T01:20:00", "count": 5}
{"event_time": "2020-05-05T01:26:00", "count": 3}
{"event_time": "2020-05-05T01:15:00", "count": 2}

>> Watermark = 01:01
{"event_time": "2020-05-05T01:00:53", "count": 2} >> Was included!!!! The window was created!!!!!!!!!
                                                    And so despite the fact of being behind the watermark


{"event_time": "2020-05-05T01:50:00", "count": 6}
>> Watermark = 01:25
{"event_time": "2020-05-05T01:22:00", "count": 2} >> Was included!!!!!!

{"event_time": "2020-05-05T02:00:00", "count": 15}
>> Watermark = 01:35

>> Try now to include late data for the window 01:20 - 01:30
>> Put a big number to see if it's included
{"event_time": "2020-05-05T01:28:00", "count": 300}

>> And you will see, it's not included because it falls before the watermark and the window for it
>> is not there anymore!

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
