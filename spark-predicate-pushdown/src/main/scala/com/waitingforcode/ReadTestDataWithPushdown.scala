package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ReadTestDataWithPushdown {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark predicate pushdown").master("local[*]")
      .getOrCreate()

    val queryWithDateTimeFilter = sparkSession.read.parquet(OutputDir)
      .filter("creationDate >= '2022-01-01'")
    queryWithDateTimeFilter.explain(true)
    queryWithDateTimeFilter.show(false)
  }

}
