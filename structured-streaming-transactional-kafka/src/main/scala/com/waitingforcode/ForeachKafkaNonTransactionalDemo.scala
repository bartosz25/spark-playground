package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ForeachKafkaNonTransactionalDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Foreach Kafka non transactional demo").master("local[*]")
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
    .mapPartitions(letters => {
      letters.map(letter => {
        if (letter == "K" && ShouldFailOnK) {
          throw new RuntimeException("Got letter that stops the processing")
        }
        letter
      })
    })

  val writeQuery = inputKafkaRecords
    .writeStream
    .option("checkpointLocation", OutputDirCheckpointNonTransactional)
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("topic", OutputTopicNonTransactional)


  writeQuery.start().awaitTermination()

}
