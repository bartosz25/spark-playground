package com.waitingforcode.extrafiles.json

import com.waitingforcode.TestConfiguration
import org.apache.spark.sql.{SparkSession, functions}

import java.util.concurrent.TimeUnit

object StreamingJoinApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("Broadcast join in Structured Streaming - new file created").master("local[*]")
    // set a small number for the shuffle partitions to avoid too many debug windows opened
    // (shuffle will happen during the join)
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.sources.useV1SourceList", "")
    .getOrCreate()
  import sparkSession.implicits._

  val currentTime = System.currentTimeMillis()
  val timestampsDataset = sparkSession.readStream.schema(
    sparkSession.read.json(TestConfiguration.datasetPath).schema
  ).format("json").load(TestConfiguration.datasetPath)
  .withColumn("watermark_col_json",
    functions.timestamp_seconds(functions.lit(TimeUnit.MILLISECONDS.toSeconds(currentTime)))) //functions.lit(currentTime).cast("timestamp"))
  .withWatermark("watermark_col_json", "1 second")



  val kafkaSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("client.id", "broadcast_join_demo_new_file_created_client")
    .option("subscribe", "broadcast_join_demo_new_file_created")
    .option("startingOffsets", "LATEST")
    .load()
    .withColumn("watermark_col_kafka",
      functions.timestamp_seconds(functions.lit(TimeUnit.MILLISECONDS.toSeconds(currentTime)))) //functions.lit(currentTime).cast("timestamp"))
    .withWatermark("watermark_col_kafka", "1 second")

  val writeQuery = kafkaSource.selectExpr("CAST(value AS STRING) AS value_key", "watermark_col_kafka")
    .join(timestampsDataset,
      $"value_key" === $"nr" && $"watermark_col_kafka" === $"watermark_col_json", "left_outer")
    .filter($"value_key".isNotNull)
    .writeStream
    .format("console")
    .option("truncate", false)

  val startedQuery = writeQuery.start()
  startedQuery.awaitTermination()
}
