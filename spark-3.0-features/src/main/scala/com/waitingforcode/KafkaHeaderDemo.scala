package com.waitingforcode

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.spark.sql.SparkSession

object KafkaHeaderDemo extends App {


  import scala.collection.JavaConverters._
  val configuration = new Properties()
  configuration.setProperty("bootstrap.servers", "localhost:29092")
  configuration.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  configuration.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](configuration)
  (0 to 10).foreach(nr => {
    producer.send(new ProducerRecord[String, String](
      "raw_data", 0,
      s"key${nr}", s"value${nr}", Seq[Header](new RecordHeader("generation_context", "localhost".getBytes)).asJava
    ))
  })
  producer.flush()

  val testSparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Kafka headers test")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  val kafkaReaderWithHeaders = testSparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "raw_data")
    .option("includeHeaders", "true")
    .option("startingOffsets", "earliest")
    .load()

  kafkaReaderWithHeaders.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
    .writeStream.format("console").option("truncate", false).start().awaitTermination()

}
