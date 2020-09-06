package com.waitingforcode.source.file

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object ArchiveStrategy extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: File source").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  val dataDir = "/tmp/file-source/data-archive-strategy"
  val archiveDir = "/tmp/file-source/archive"
  FileUtils.deleteDirectory(new File(dataDir))
  new Thread(new FilesGenerator(dataDir)).start()

  val writeQuery = sparkSession.readStream
    .option("cleanSource", "archive")
    .option("sourceArchiveDir", archiveDir)
    .text(dataDir)
    .writeStream
    .format("console")
    .option("truncate", false)

  writeQuery.start().awaitTermination()
  FileUtils.deleteDirectory(new File(dataDir))
}
