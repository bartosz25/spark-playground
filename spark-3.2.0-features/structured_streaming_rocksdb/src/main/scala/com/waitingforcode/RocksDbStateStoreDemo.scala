package com.waitingforcode

import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions}

object RocksDbStateStoreDemo extends App {

  val checkpointLocation = "/tmp/wfc/structured-streaming-rocksdb"

  val sparkSession = SparkSession.builder()
    .appName("RocksDB state store demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2) // 2 physical state stores; one per shuffle partition task
    .config("spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
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
    .dropDuplicates(Seq("event_time", "id"))

  val writeQuery = window
    .writeStream
    .option("checkpointLocation", checkpointLocation)
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()

}
