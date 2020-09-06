package com.waitingforcode.source.file

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object DeleteStrategy extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: File source").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  val dataDir = "/tmp/file-source/data-delete-strategy"
  FileUtils.deleteDirectory(new File(dataDir))
  new Thread(new FilesGenerator(dataDir)).start()


  val writeQuery = sparkSession.readStream
    .option("cleanSource", "delete")
    .text(dataDir)
    .writeStream
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()
  FileUtils.deleteDirectory(new File(dataDir))
}
