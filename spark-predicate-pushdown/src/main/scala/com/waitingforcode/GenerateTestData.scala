package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File
import java.sql.Timestamp

object GenerateTestData {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(OutputDir))

    val sparkSession = SparkSession.builder()
      .appName("Spark predicate pushdown").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val inputData = Seq(Letter("a", "A", new Timestamp(1L)),
      Letter("b", "B", new Timestamp(1651356000000L)),
      Letter("c", "C", new Timestamp(100))).toDF

    inputData.write.mode(SaveMode.Overwrite).parquet(OutputDir)
  }

}
