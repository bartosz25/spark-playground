package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

// I'm not using Derby here because it cannot be accessed concurrently!
// That's why, I had to launch a remote Hive metastore
// used https://github.com/arempter/spark_hive_test and https://medium.com/@adamrempter/running-spark-3-with-standalone-hive-metastore-3-0-b7dfa733de91
// TODO: add note in the repo about /spark dir in minio volume!
// cd /home/bartosz/workspace/hive-metastore-docker
// gedit docker-compose.yml
// >> describe the compose
// >> say that you added a /spark directory in the miniio
// docker-compose down --volumes; docker-compose up
object StructuredStreamingTableWriter extends App {

  val demoWarehouseDir = "/tmp/wfc/spark-3.1.1/table-writer"
  FileUtils.deleteDirectory(new File(demoWarehouseDir))
  val checkpointDir = "/tmp/wfc/spark3.1/table/writer/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val sparkSession = SparkSession.builder()
    .appName("Table writer").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    // Configuration for the external metastore
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.hadoop.fs.s3a.access.key", "accesskey")
    .config("spark.hadoop.fs.s3a.secret.key", "secretkey")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .enableHiveSupport()
    .getOrCreate()

  val rateStream = sparkSession.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()

  val writeQuery = rateStream.writeStream
    .format("json")
    .option("checkpointLocation", checkpointDir)
    .toTable("rate_table2")

  writeQuery.awaitTermination()
}
