package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object FileSourceCompactDemo extends App {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file source example")
    .master("local[2]")
    // I set a very small compactInterval to show an accelerated compaction process
    .config("spark.sql.streaming.fileSource.log.compactInterval", "2")
    // Let's keep only 2 last batches, logically, we should only keep 2 last
    // metadata files but it won't be the case because
    // the compacted file is unaware of this configuration!
    .config("spark.sql.streaming.minBatchesToRetain", "2")
    // We don't deal with eventual consistency issues, set this to 0
    // in order to remove the "expired" files as soon as the expiration
    // time is reached
    .config("spark.sql.streaming.fileSource.log.cleanupDelay", 0)
    .getOrCreate()

  val streamDir = "/tmp/wfc/file-source-oom-error/"
  val dataDir = s"${streamDir}/data"
  FileUtils.deleteDirectory(new File(dataDir))
  new Thread(new Runnable {
    override def run(): Unit = {
      var nr = 1
      while (nr <= 4000) {
        val fileName = s"${dataDir}/${nr}.txt"
        val jsonContent = nr.toString
        FileUtils.writeStringToFile(new File(fileName), jsonContent)
        nr += 1
        Thread.sleep(5000L)
      }
    }
  }).start()

  val checkpointLocation = s"${streamDir}/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointLocation))
  val writeQuery = sparkSession.readStream.text(dataDir).writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", s"file://${checkpointLocation}")

  writeQuery.start().awaitTermination(120000)

}
