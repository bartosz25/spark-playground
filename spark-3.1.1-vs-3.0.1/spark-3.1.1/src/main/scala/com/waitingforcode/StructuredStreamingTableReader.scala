package com.waitingforcode

import org.apache.spark.sql.SparkSession

object StructuredStreamingTableReader extends App {

  val demoWarehouseDir = "/tmp/wfc/spark-3.1.1/table-writer"
  val sparkSession = SparkSession.builder()
    .appName("Table reader").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.hadoop.fs.s3a.access.key", "accesskey")
    .config("spark.hadoop.fs.s3a.secret.key", "secretkey")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .enableHiveSupport()
    .getOrCreate()

  val rateStreamTable = sparkSession.readStream
    .table("rate_table2")

  val writeQuery = rateStreamTable.writeStream.format("console").option("truncate", false)

  writeQuery.start().awaitTermination()
}
