package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.io.File

object Demo1DataGeneration {

  val OutputDataDir = "/tmp/spark-wildcard-reading/input/subdir"

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(OutputDataDir))
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val rawInput = (0 to 5).map(nr => (nr, s"label=${nr}", nr % 3 == 0)).toDF("nr", "label", "modulo")

    Seq("a", "b", "c").foreach(partitionValue => {
      rawInput.withColumn("partition_value", functions.lit(partitionValue))
        .write.partitionBy("partition_value").mode(SaveMode.Append).json(OutputDataDir)
    })
  }

}
