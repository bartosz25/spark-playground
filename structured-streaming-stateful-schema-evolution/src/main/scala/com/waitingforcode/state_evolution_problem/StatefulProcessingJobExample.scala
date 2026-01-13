package com.waitingforcode.state_evolution_problem

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.classic.{KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, TimeMode, Trigger}
import org.apache.spark.sql.types.StructType

import java.io.File
import java.util.TimeZone

object StatefulProcessingJobExample {

  def main(args: Array[String]): Unit = {
    println("Cleaning the demo context...")
    println("....the files")
    FileUtils.deleteDirectory(new File(DataGenerator.OutputPath))
    println("Cleaned!")
    println("...and the checkpoint location")
    FileUtils.deleteDirectory(new File(CheckpointLocation))
    println("Cleaned!")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.shuffle.partitions", 1).config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      //.config("spark.sql.streaming.stateStore.stateSchemaCheck", schemaCheckEnabled)
      //.config("spark.sql.streaming.stateStore.encodingFormat", "avro")
      .getOrCreate()
    import sparkSession.implicits._

    DataGenerator.generate(SaveMode.Overwrite)

    val inputStream = sparkSession.readStream
      .schema(ScalaReflection.schemaFor[Letter].dataType.asInstanceOf[StructType])
      .option("maxFilesPerTrigger", 1).json(DataGenerator.OutputPath).as[Letter]

    val groupedLetters: KeyValueGroupedDataset[Int, Letter] = inputStream
      .groupByKey(row => row.id)

    val lettersStateOutput = groupedLetters.transformWithState(
      statefulProcessor=new LetterProcessorV1(),
      timeMode=TimeMode.None(),
      outputMode=OutputMode.Update()
    )

    val query = lettersStateOutput.writeStream.format("console")
      .option("checkpointLocation", CheckpointLocation)
      .trigger(Trigger.AvailableNow())
      .outputMode(OutputMode.Update())
      .option("truncate", false).start()

    query.awaitTermination()
  }

}
