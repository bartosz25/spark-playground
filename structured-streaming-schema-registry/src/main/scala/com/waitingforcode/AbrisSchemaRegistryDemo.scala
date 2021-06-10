package com.waitingforcode

import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

object AbrisSchemaRegistryDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("ABRiS demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KafkaBrokerUlr)
    .option("client.id", "abris_demo_client")
    .option("subscribe", DemoTopicName)
    .option("startingOffsets", "EARLIEST")
    .load()

  val avroOrders = inputKafkaRecords
    .select(from_avro($"value", AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(DemoTopicName)
      .usingSchemaRegistry(SchemaRegistryUrl)).as("record"))
    .selectExpr("record.*")

  val writeQuery = avroOrders
    .writeStream
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()

}
