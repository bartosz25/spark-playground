package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}

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
{"id": 1, "eventTime": "2020-01-01T10:16:00"}
==> watermark 10:10

2) No match - too late
left:
{"id": 3, "eventTime": "2020-01-01T11:18:00"}

right
{"id": 3, "eventTime": "2020-01-01T11:28:00"}

==> watermark: 11:13 and the matched rows are not returned twice! (optimization from Spark 2.4)
3) No match => emit previously not matched rows by moving the watermark on the right side:
left:
{"id": 4, "eventTime": "2020-01-01T11:45:00"}
right:
{"id": 5, "eventTime": "2020-01-01T11:48:00"}
==> watermark 11:38

4) Some other events to move the watermark on and emit the rows from the point 3
left:
{"id": 7, "eventTime": "2020-01-01T12:10:00"}

right:
{"id": 8, "eventTime": "2020-01-01T12:15:00"}

 */

object FullOuterJoinStructuredStreaming311 extends App {

  val session = SparkSession.builder()
    .appName("Full-outer join").master("local[*]")
    .config("spark.sql.shuffle.partitions", 1)
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


  val query = leftSide.withWatermark("leftTime", "5 minutes")
    .join(
      rightSide.withWatermark("rightTime", "10 minutes"),
      // right event has to come within 2 minutes regarding the right event
      functions.expr(
        "rightId = leftId AND rightTime <= leftTime + interval 2 minutes "
         + " AND rightTime >= leftTime"
      ),
      joinType = "full_outer"
    )
    .writeStream
    .format("console")
    .option("truncate", false)
    .start()

  query.awaitTermination()

}
