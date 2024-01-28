package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.streaming.Trigger

import java.time.LocalDateTime

/**
 * Start the Docker container:
 * ```
 * cd broker
 * docker-compose down --volumes; docker-compose up
 * ```
 *
 * Set the context up:
 * 1. docker exec -ti docker_kafka_1  kafka-topics.sh --bootstrap-server localhost:9092 --topic numbers --partitions 1 --create
 * 2. docker exec -ti docker_kafka_1  kafka-console-producer.sh --broker-list localhost:9094 --topic numbers
 * 3. Create 4 records: 1, 2, 3, 4
 * 4. Run the job. The job should process the 4 records.
 */
object MinOffsetsForAvailableNow extends App {

  val sparkSession = SparkSession.builder().master("local[*]")
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()

  println(s"Starting time ${LocalDateTime.now()}")
  val kafkaSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", "numbers")
    .option("minOffsetsPerTrigger", 2000)
    .option("maxTriggerDelay", "30m")
    .option("startingOffsets", "EARLIEST")
    .load().withColumn("current_time", functions.current_timestamp())

  val writeQuery = kafkaSource.selectExpr("CAST(value AS STRING) AS value_key", "current_time")
    .writeStream
    .trigger(Trigger.AvailableNow())
    .format("console")
    .option("truncate", false)

  val startedQuery = writeQuery.start()
  startedQuery.awaitTermination()

}
