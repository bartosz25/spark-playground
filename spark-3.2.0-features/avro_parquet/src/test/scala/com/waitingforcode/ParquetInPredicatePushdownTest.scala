package com.waitingforcode

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class ParquetInPredicatePushdownTest extends AnyFlatSpec  {

  // It's a test without assertions but with print, so that it can be run
  // with Maven profiles to check the executed plans
  it should "pushdown IN predicate as between for Spark 3.2.0" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-32792 : improved IN predicate pushdown").master("local[*]")
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.sql.parquet.pushdown.inFilterThreshold", 2)
      .getOrCreate()

    val outputDir = "/tmp/parquet-in-predicate"
    sparkSession.range(1, 4000).toDF("number").write.mode(SaveMode.Overwrite).save(outputDir)

    sparkSession.read.parquet(outputDir).filter("number IN (2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24)")
      .explain(true)
  }

}
