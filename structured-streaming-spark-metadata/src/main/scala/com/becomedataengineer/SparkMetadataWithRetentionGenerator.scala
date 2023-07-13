package com.becomedataengineer

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.util.TimeZone

object SparkMetadataWithRetentionGenerator {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.minBatchesToRetain", 10)
      .getOrCreate()
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val startTimestamp = Timestamp.valueOf("2023-07-01 09:00:00")
    val inputStream = sparkSession.readStream.format("rate-micro-batch")
      // To better simulate the issue, let's define a lot of partitions
      .option("numPartitions", 15)
      .option("rowsPerBatch", 30)
      .option("advanceMillisPerBatch", 60000)
      .option("startTimestamp", startTimestamp.getTime)
      .load()

    val writeQuery = inputStream
      .writeStream
      .format("json")
      .option("retention", "5s")
      .option("checkpointLocation",  "/tmp/wfc/spark_metadata_retention/checkpoint")
      .option("path", "/tmp/wfc/spark_metadata_retention/data")
      .start()

    writeQuery.awaitTermination()
  }
}
