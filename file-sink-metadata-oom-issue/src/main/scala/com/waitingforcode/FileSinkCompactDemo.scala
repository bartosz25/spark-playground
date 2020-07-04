package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream

object FileSinkCompactDemo extends App {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file sink example")
    .master("local[2]")
    // I set a very small compactInterval to show an accelerated compaction process
    .config("spark.sql.streaming.fileSink.log.compactInterval", "2")
    // Let's keep only 2 last batches, logically, we should only keep 2 last
    // metadata files for the sink but it won't be the case because
    // the compacted file is unaware of this configuration!
    .config("spark.sql.streaming.minBatchesToRetain", "2")
    .getOrCreate()
  import sparkSession.implicits._

  val inputStream = new MemoryStream[(String)](1, sparkSession.sqlContext)
  new Thread(new Runnable {
    override def run(): Unit = {
      var nr = 0
      while (true) {
        inputStream.addData(s"${nr}")
        Thread.sleep(2000L)
        nr += 1
      }
    }
  }).start()


  val baseDir = "/tmp/spark/file-sink"
  val outputPath = s"${baseDir}/output"
  FileUtils.deleteDirectory(new File(outputPath))
  val checkpointLocation = s"${baseDir}/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointLocation))
  val writeQuery = inputStream.toDS().toDF("nr").writeStream
    .format("json")
    .option("path", s"file://${outputPath}")
    .option("checkpointLocation", s"file://${checkpointLocation}")

  writeQuery.start().awaitTermination(120000)

}
