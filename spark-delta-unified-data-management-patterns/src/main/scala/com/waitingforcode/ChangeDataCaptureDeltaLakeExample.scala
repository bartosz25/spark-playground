package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object ChangeDataCaptureDeltaLakeExample extends App {

  val inputDir = "/tmp/delta-cdc"
  val localSparkSession: SparkSession = SparkSession.builder()
    .appName("[Delta Lake] CDC with Structured Streaming").master("local[*]").getOrCreate()

  val inputSource = localSparkSession.readStream.format("delta").load(inputDir)

  val writeQuery = inputSource.writeStream.format("console")
    .start()

  writeQuery.awaitTermination()
}

object DataProducer extends App {

  val outputDir = "/tmp/delta-cdc"
  FileUtils.deleteDirectory(new File(outputDir))
  val localSparkSession: SparkSession = SparkSession.builder()
    .appName("[Delta Lake] CDC producer").master("local[*]").getOrCreate()

  val inputSource = localSparkSession.readStream
    .format("rate")
    .option("numPartitions", 1)
    .option("rampUpTime", "2s")
    .option("rowsPerSecond", 1)
    .load()

  val writeQuery = inputSource.writeStream
    .format("delta")
    .option("checkpointLocation", s"${outputDir}/checkpoint")
    .start(outputDir)

  writeQuery.awaitTermination()
}