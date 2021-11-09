package com.waitingforcode

import org.apache.spark.sql.SparkSession

object KafkaLatencyMetricsDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Kafka changes demo").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "kafka_changes")
    .option("maxOffsetsPerTrigger", 2) // To avoid taking the most recent records and hence miss the *BehindLatest metric
    .load()

  val inputSource = inputKafkaRecords.selectExpr("CAST(value AS STRING)").as[String]
    .map(letter => {
      // I'm sleeping here to simulate a slow processing function and see the *BehindLatest metrics increase
      Thread.sleep(5000L)
      s"Sleeping ${letter}"
    })

  val writeQuery = inputSource
    .writeStream
    .format("console")
    .option("truncate", false)

  inputSource.explain(true)

  writeQuery.start().awaitTermination()

}
