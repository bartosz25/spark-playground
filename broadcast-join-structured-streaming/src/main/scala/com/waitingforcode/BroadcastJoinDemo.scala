package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object TestConfiguration {
  val datasetPath = "/tmp/wfc/broadcast-join/test_dataset"
}

// Run DataOverwrite to generate the first version of the files

// Later, setup Kafka context:
// docker exec -ti broker_kafka_1 bin/bash
// kafka-topics.sh --bootstrap-server localhost:9092 --topic broadcast_join_demo --delete

// Next, create with only 2 partitions for easier debugging
// kafka-topics.sh --bootstrap-server localhost:9092 --topic broadcast_join_demo --partitions 2 --create
// kafka-console-producer.sh --broker-list localhost:9094 --topic broadcast_join_demo
object BroadcastJoinDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Broadcast join in Structured Streaming").master("local[*]")
    // set a small number for the shuffle partitions to avoid too many debug windows opened
    // (shuffle will happen during the join)
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()
  import sparkSession.implicits._

  val timestampsDataset = sparkSession.read.json(TestConfiguration.datasetPath)

  val kafkaSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("client.id", "broadcast_join_client")
    .option("subscribe", "broadcast_join_demo")
    .option("startingOffsets", "EARLIEST")
    .load()

  val writeQuery = kafkaSource.selectExpr("CAST(value AS STRING) AS value_key")
    .join(timestampsDataset, $"value_key" === $"nr", "left_outer")
    .writeStream
    .format("console")
    .option("truncate", false)

  val startedQuery = writeQuery.start()
  startedQuery.awaitTermination()

}

object DataOverwriter extends App {

  val version1 = s"""
                    |{"nr": 0, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                    |{"nr": 2, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                    |{"nr": 4, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                    |{"nr": 6, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                    |""".stripMargin
  val version2 = s"""
                    |{"nr": 0, "is_even": true, "new_field": "a", "ts": "${System.currentTimeMillis()}"}
                    |{"nr": 2, "is_even": true, "new_field": "a","ts": "${System.currentTimeMillis()}"}
                    |{"nr": 4, "is_even": true, "new_field": "a","ts": "${System.currentTimeMillis()}"}
                    |{"nr": 6, "is_even": true, "new_field": "a","ts": "${System.currentTimeMillis()}"}
                    |""".stripMargin

  val versionToWrite = version1

  // To illustrate that the new dataset is taken, hence the batch dataset is read,
  // replace the `versionToWrite` by `version2` and rerun the generator

  FileUtils.deleteDirectory(new File(TestConfiguration.datasetPath))
  FileUtils.writeStringToFile(new File(s"${TestConfiguration.datasetPath}/test.json"), versionToWrite)
}
