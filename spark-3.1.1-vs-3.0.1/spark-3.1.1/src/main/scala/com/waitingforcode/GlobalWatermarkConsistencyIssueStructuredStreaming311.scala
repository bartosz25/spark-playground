package com.waitingforcode

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

import scala.concurrent.duration.DurationInt

// Demo steps + enable debugging
/*
recreate topics:
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic stream1 --delete &&
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic stream2 --delete &&
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic stream3 --delete &&
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic stream1 --partitions 2 --create &&
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic stream2 --partitions 2 --create
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic stream3 --partitions 2 --create
 */
object GlobalWatermarkConsistencyIssueStructuredStreaming311 extends App {

  val session = SparkSession.builder()
    .appName("Global watermark consistency issue").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", true)
    .getOrCreate()
  import session.implicits._

  val stream1 = session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "stream1")
    .option("startingOffsets", "EARLIEST")
    .load()
    .select(functions.from_json($"value".cast("string"), EventLog.Schema).as("value"))
    .select($"value.id".as("leftId"), $"value.eventTime".as("leftTime"))

  val stream2 = session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "stream2")
    .option("startingOffsets", "EARLIEST")
    .load()
    .select(functions.from_json($"value".cast("string"), EventLog.Schema).as("value"))
    .select($"value.id".as("rightId"), $"value.eventTime".as("rightTime"))

  val stream3 = session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "stream3")
    .option("startingOffsets", "EARLIEST")
    .load()
    .select(functions.from_json($"value".cast("string"), EventLog.Schema).as("value"))
    .select($"value.id".as("rightId2"), $"value.eventTime".as("rightTime2"))

  val query = stream1.withWatermark("leftTime", "5 minutes")
    .join(
      stream2.withWatermark("rightTime", "10 minutes"),
      functions.expr(
        "rightId = leftId AND  rightTime = leftTime"
      ),
      joinType = "left_outer"
    ).withWatermark("leftTime", "20 minutes").join(
      stream3.withWatermark("rightTime2", "15 minutes"),
      functions.expr(
        "rightId2 = leftId AND  rightTime2 = leftTime AND rightTime2 = rightTime"
      ),
      joinType = "left_outer"
  ).withWatermark("leftTime", "20 minutes")


  val writeQuery = query.writeStream
    .trigger(Trigger.ProcessingTime(2.seconds))
    .format("console")
    .option("truncate", false)
    .start()

  writeQuery.awaitTermination()

}
