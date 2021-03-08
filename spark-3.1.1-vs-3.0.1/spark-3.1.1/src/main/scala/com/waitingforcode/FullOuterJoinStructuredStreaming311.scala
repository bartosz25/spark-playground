package com.waitingforcode

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

import scala.concurrent.duration.DurationInt

object FullOuterJoinStructuredStreaming311 extends App {

  val session = SparkSession.builder()
    .appName("Full-outer join").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()
  import session.implicits._

  val leftSide = session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "left_side")
    .option("startingOffsets", "EARLIEST")
    .load()
    .select(functions.from_json($"value".cast("string"), EventLog.Schema).as("value"))
    .select($"value.id".as("leftId"), $"value.eventTime".as("leftTime"))

  val rightSide = session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "right_side")
    .option("startingOffsets", "EARLIEST")
    .load()
    .select(functions.from_json($"value".cast("string"), EventLog.Schema).as("value"))
    .select($"value.id".as("rightId"), $"value.eventTime".as("rightTime"))


  // Demo steps:
  /*
  recreate topics:
  docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic left_side --delete &&
  docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic right_side --delete &&
  docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic left_side --partitions 2 --create &&
  docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic right_side --partitions 2 --create

  # tab#1
  docker exec -ti broker_kafka_1 kafka-console-producer.sh --broker-list localhost:29092 --topic left_side
  # tab#2
  docker exec -ti broker_kafka_1 kafka-console-producer.sh --broker-list localhost:29092 --topic right_side

1) On-time match
left:
{"id": 1, "eventTime": "2020-01-01T10:15:00"}
right:
{"id": 1, "eventTime": "2020-01-01T10:20:00"}
2) Late match
left:
{"id": 2, "eventTime": "2020-01-01T11:15:00"}
right:
{"id": 2, "eventTime": "2020-01-01T11:36:00"}
3) No match
left:
{"id": 3, "eventTime": "2020-01-01T11:18:00"}
left (to move watermark forward):
{"id": 5, "eventTime": "2020-01-01T12:45:00"}
right
{"id": 6, "eventTime": "2020-01-01T12:50:00"}
4) No match => emit previously not matched rows:
left:
{"id": 7, "eventTime": "2020-01-01T13:45:00"}
right:
{"id": 8, "eventTime": "2020-01-01T13:45:00"}
   */
  val query = leftSide.withWatermark("leftTime", "5 minutes")
    .join(
      rightSide.withWatermark("rightTime", "10 minutes"),
      functions.expr(
        // Expect the rightTime to happen at the same time or later than the leftTime
        // and so within at most 20 minutes
        "rightId = leftId AND  rightTime >= leftTime"
      ),
      joinType = "full_outer"
    )
    .writeStream
    .trigger(Trigger.ProcessingTime(2.seconds))
    .format("console")
    .option("truncate", false)
    .start()

  query.awaitTermination()

}
