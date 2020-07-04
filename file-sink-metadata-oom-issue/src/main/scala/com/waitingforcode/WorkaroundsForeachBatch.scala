package com.waitingforcode

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql._

import scala.collection.JavaConverters._

object FileSinkWorkaroundsForeachBatch extends App {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file sink workarounds - weak semantic")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._
  val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)

  val stream = inputStream.toDS().toDF("number")

  val query = stream.writeStream
    .foreachBatch((dataset: DataFrame, batchId: Long) => {
      dataset.write.mode(SaveMode.Append).json("/tmp/spark/file-sink/workaround-1/")
    })
    .start()

  query.awaitTermination(20000L)

}

object FileSinkWorkaroundsForeachBatchExactlyOnceSemantic extends App {

  val baseDir = "/tmp/spark/file-sink/workaround-2/"
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file sink workarounds - enforced semantic")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._
  val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
  new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        inputStream.addData(Seq(1, 2, 3, 4))
        Thread.sleep(2000)
      }
    }
  }).start()

  val stream = inputStream.toDS().toDF("number")

  val query = stream.writeStream.option("checkpointLocation", s"${baseDir}/checkpoint${System.currentTimeMillis()}")
    .foreachBatch((dataset: DataFrame, batchId: Long) => {
      writeData(dataset, batchId)
    })
    .start()

  query.awaitTermination()

  def writeData(dataset: Dataset[Row], batchId: Long): Unit = {
    val stagingDir = s"${baseDir}/${batchId}"
    dataset.write.mode(SaveMode.Overwrite).json(stagingDir)

    /**
      * Now the idea is to:
      * 1) Get the metadata file, you can get it from the batchId number
      * 2) Check the metadata file:
      *    - if it contains the ${batchId}, read all entries and delete them from disk and metadata file
      *    - otherwise, do nothing
      * 3) List all file from the ${temporaryDir} and add them to the metadata file
      * 4) Move the files from the ${temporaryDir} to the final location
      *
      * You can also move this code to another place to avoid to
      * slow down the streaming writing. But the trade-off for that is the
      * need to monitor this extra process and restart it if needed.
      * I put it here for the sake of simplicity. For the same reason, I'm using the
      * local file system as well.
      *
      * Of course, it remains a simplified version because in addition to that, we should manage
      * the 3rd and 4th points atomically and also implement a support for partitioning columns.
      */
    val metadataDir = s"${baseDir}/metadata"
    val outputDir = s"${baseDir}/output"
    new ExactlyOnceWriter(batchId, metadataDir, outputDir, stagingDir)
      .cleanPreviousBatchExecution
      .promoteStagingFiles
  }
}


class ExactlyOnceWriter(batchId: Long, metadataDir: String, outputDir: String, stagingDir: String) {

  type StagingFileWithNewName = (File, String)

  lazy val stagingFiles = FileUtils.listFiles(new File(stagingDir), Array("json"), true)

  lazy val stagingFilesWithNewNames: Iterable[StagingFileWithNewName] = stagingFiles.asScala.zipWithIndex
    .map(stagingFileWithIndex => {
      val (stagingFile, index) = stagingFileWithIndex
      (stagingFile, s"${outputDir}/batch-${batchId}-${index}.json")
    })

  private val metadataFileNumber = {
    val entriesPerFile = 100
    batchId / 100
  }
  private val metadataFile = new File(s"${metadataDir}/${metadataFileNumber}.meta")
  lazy val metadataFileEntity = {
    if (metadataFile.exists()) {
      ExactlyOnceWriter.JacksonMapper.readValue(metadataFile, classOf[MetadataFile])
    } else {
      MetadataFile()
    }
  }

  def cleanPreviousBatchExecution: this.type = {
    metadataFileEntity.files.get(batchId.toString).map(oldFiles => {
      oldFiles.foreach(oldFile => new File(oldFile).delete())
    })
    this
  }

  def promoteStagingFiles: this.type = {
    stagingFilesWithNewNames.foreach {
      case (stagingFile, newName) => {
        FileUtils.moveFile(stagingFile, new File(newName))
      }
    }
    val newMetadataFiles = metadataFileEntity.files + (batchId.toString -> stagingFilesWithNewNames.map(_._2))
    println(newMetadataFiles)
    FileUtils.writeStringToFile(metadataFile,
      ExactlyOnceWriter.JacksonMapper.writeValueAsString(metadataFileEntity.copy(files = newMetadataFiles))
    )
    this
  }

}

object ExactlyOnceWriter {
  val JacksonMapper = new ObjectMapper() with ScalaObjectMapper
  JacksonMapper.registerModule(DefaultScalaModule)
}

// use String because of https://github.com/FasterXML/jackson-module-scala/issues/219
case class MetadataFile(files: Map[String, Iterable[String]] = Map.empty[String, Iterable[String]])
