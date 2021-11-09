package com.waitingforcode

import org.apache.spark.sql.functions.session_window
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions}

object SessionWindow extends App {


  val sparkSession = SparkSession.builder()
    .appName("Session window demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2) // 2 physical state stores; one per shuffle partition task
    .getOrCreate()

  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "rocksdb_state_store")
    .option("startingOffsets", "LATEST")
    .load()

  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType)
  ))

  val window = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "25 minutes")
    .groupBy(session_window($"event_time", "10 seconds") as 'session, 'id)
    .agg(functions.count("*").as("numEvents"))
    .selectExpr("id", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
      "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
      "numEvents")


  val writeQuery = window
    .writeStream
    .format("console")
    .option("truncate", false)

  window.explain(true)

  writeQuery.start().awaitTermination()

}
