package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}

// Demo steps:
/*
recreate topics:
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic metrics_demo --delete &&
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic metrics_demo --partitions 2 --create &&
docker exec -ti broker_kafka_1 kafka-console-producer.sh --broker-list localhost:29092 --topic metrics_demo

{"id": 1, "eventTime": "2020-01-01T10:15:00"}
==> watermark: 10:13:00

=> late event, aggregated number of rows dropped by the watermark should increase // state store rows the same
{"id": 2, "eventTime": "2020-01-01T10:12:00"}

=> duplicate, "dropped by" doesn't increase, but number of state store rows neither which is good!
dropped means for the late data
{"id": 1, "eventTime": "2020-01-01T10:15:00"}


==> new events to move the watermark to remove the rows and decrease state store
{"id": 3, "eventTime": "2020-01-01T11:18:00"}
--> number of state rows increases to 2 and just after decreases to 1 because the id 1 was
removed due to an old watermark

// check late event ==> dropped by watermark increases
{"id": 4, "eventTime": "2020-01-01T11:10:00"}
 */
object StateStoreMetrics extends App {

  val sparkSession = SparkSession.builder()
    .appName("State store metrics").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  import sparkSession.implicits._

  val metricsSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "metrics_demo")
    .option("startingOffsets", "EARLIEST")
    .load()
    .select(functions.from_json($"value".cast("string"), EventLog.Schema).as("value"))
    .select($"value.*")


  val deduplicatedInput = metricsSource
    .withWatermark("eventTime", "2 minutes")
    .dropDuplicates("id", "eventTime")

  val writeQuery = deduplicatedInput
    .writeStream
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()

}