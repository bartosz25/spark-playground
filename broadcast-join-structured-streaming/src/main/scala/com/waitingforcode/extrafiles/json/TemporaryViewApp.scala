package com.waitingforcode.extrafiles.json

import com.waitingforcode.TestConfiguration
import org.apache.spark.sql.SparkSession

import java.io.File

object TemporaryViewApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("Broadcast join in Structured Streaming - new file created").master("local[*]")
    // set a small number for the shuffle partitions to avoid too many debug windows opened
    // (shuffle will happen during the join)
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.sources.useV1SourceList", "")
    .getOrCreate()
  import sparkSession.implicits._

  sparkSession.read.json(TestConfiguration.datasetPath).createOrReplaceGlobalTempView("timestamps_to_join")
  sparkSession.sql("SELECT * FROM global_temp.timestamps_to_join").show()
  new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        if (new File(s"${TestConfiguration.datasetPath}/_SUCCESS").exists()) {
          println("Refreshing timestamps_to_join")
          sparkSession.read.json(TestConfiguration.datasetPath).createOrReplaceGlobalTempView("timestamps_to_join")
          sparkSession.sql("SELECT * FROM global_temp.timestamps_to_join").show()
        }
        Thread.sleep(5000L)
      }
    }
  }).start()


  val kafkaSource = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("client.id", "broadcast_join_demo_new_file_created_client")
    .option("subscribe", "broadcast_join_demo_new_file_created")
    .option("startingOffsets", "LATEST")
    .load()

  val writeQuery = kafkaSource.selectExpr("CAST(value AS STRING) AS value_key")
    .join(sparkSession.sql("SELECT * FROM global_temp.timestamps_to_join"), $"value_key" === $"nr",
      "left_outer")
    .writeStream
    .format("console")
    .option("truncate", false)

  val startedQuery = writeQuery.start()
  startedQuery.awaitTermination()
}
