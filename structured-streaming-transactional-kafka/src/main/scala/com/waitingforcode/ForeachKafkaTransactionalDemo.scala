package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ForeachKafkaTransactionalDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Foreach Kafka transactional demo").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._
  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", InputTopicName)
    .option("startingOffsets", "EARLIEST")
    .option("maxOffsetsPerTrigger", 10)
    .load()
    .selectExpr("CAST(value AS STRING)").as[String]

  val writeQuery = inputKafkaRecords
    .writeStream
    .option("checkpointLocation", OutputDirCheckpointTransactional)
    .foreach(new ForeachKafkaTransactionalWriter(OutputTopicTransactional))

  writeQuery.start().awaitTermination()

}
