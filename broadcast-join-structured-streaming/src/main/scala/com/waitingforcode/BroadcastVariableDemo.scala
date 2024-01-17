package com.waitingforcode

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}


// Steps to run the demo:
// docker exec -ti broker_kafka_1 bin/bash
// kafka-topics.sh --bootstrap-server localhost:9092 --topic broadcast_variable_demo --delete

// Next, create with only 2 partitions for easier debugging
// kafka-topics.sh --bootstrap-server localhost:9092 --topic broadcast_variable_demo --partitions 2 --create
// kafka-console-producer.sh --broker-list localhost:9094 --topic broadcast_variable_demo
object BroadcastVariableDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Broadcast variable demo in Structured Streaming").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  // I'm running this process to change the variable and see whether
  // it's broadcasted to the micro-batch query
  var commonLabel = "abc -> "
  new Thread(() => {
    while (true) {
      commonLabel += "."
      println(s"Common label after change=${commonLabel}")
      Thread.sleep(5000)
    }
  }).start()
  val commonLabelBroadcast = sparkSession.sparkContext.broadcast(commonLabel)

  val kafkaSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("client.id", "broadcast_variable_client")
    .option("subscribe", "broadcast_variable_demo")
    .option("startingOffsets", "EARLIEST")
    .load()

  def decorateValueKeyWithLabel(row: Row, label: Broadcast[String]): String = {
    val valueKey = row.getAs[String]("value_key")
    s"${label.value}${valueKey}"
  }

  val writeQuery = kafkaSource.selectExpr("CAST(value AS STRING) AS value_key")
    .map(row => decorateValueKeyWithLabel(row, commonLabelBroadcast))
    .writeStream
    .format("console")
    .option("truncate", false)

  val startedQuery = writeQuery.start()
  startedQuery.awaitTermination()

}
