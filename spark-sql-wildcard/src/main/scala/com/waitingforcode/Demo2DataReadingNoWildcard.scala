package com.waitingforcode

import org.apache.spark.sql.SparkSession

object Demo2DataReadingNoWildcard {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.sources.useV1SourceList", "")
      .getOrCreate()

    sparkSession.read.json(s"${Demo1DataGeneration.OutputDataDir}").show(false)
  }

}
