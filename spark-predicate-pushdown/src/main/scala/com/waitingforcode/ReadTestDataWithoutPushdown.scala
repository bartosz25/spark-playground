package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ReadTestDataWithoutPushdown {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark predicate pushdown").master("local[*]")
      .config("spark.sql.sources.useV1SourceList", "")
      .getOrCreate()

    val queryWithDateTimeFilter = sparkSession.read.parquet(OutputDir)
      .filter("CAST(creationDate AS STRING) >= '2022-01-01'")
    //queryWithDateTimeFilter.explain(true)
    queryWithDateTimeFilter.show(false)
  }

}
