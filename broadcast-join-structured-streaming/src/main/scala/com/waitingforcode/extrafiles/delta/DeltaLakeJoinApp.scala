package com.waitingforcode.extrafiles.delta

import com.waitingforcode.TestConfiguration
import org.apache.spark.sql.SparkSession

object DeltaLakeJoinApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("Broadcast join in Structured Streaming - new file created").master("local[*]")
    // set a small number for the shuffle partitions to avoid too many debug windows opened
    // (shuffle will happen during the join)
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.sources.useV1SourceList", "")
    .getOrCreate()
  import sparkSession.implicits._

  val timestampsDataset = sparkSession.read.format("delta").load(TestConfiguration.datasetPath)

  val kafkaSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("client.id", "broadcast_join_demo_new_file_created_client")
    .option("subscribe", "broadcast_join_demo_new_file_created")
    .option("startingOffsets", "LATEST")
    .load()

  val writeQuery = kafkaSource.selectExpr("CAST(value AS STRING) AS value_key") //, "watermark_col_kafka")
    .join(timestampsDataset, $"value_key" === $"nr", "left_outer")
    .writeStream
    .format("console")
    .option("truncate", false)

  val startedQuery = writeQuery.start()
  startedQuery.awaitTermination()
}
