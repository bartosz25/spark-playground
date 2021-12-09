package com.waitingforcode

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReadParquetDataTest extends AnyFlatSpec with Matchers {

  it should "fail only for EXCEPTION mode" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34377 : Parquet date time mode at a source level").master("local[*]")
      .getOrCreate()

    val exceptionMode = intercept[SparkException] {
      sparkSession.read.format("parquet").option("datetimeRebaseMode", "EXCEPTION")
        .load("/tmp/spark245-input").show(false)
    }
    exceptionMode.getMessage should include("You may get a different result due to the upgrading of Spark 3.0: \nreading dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z from Parquet INT96\nfiles can be ambiguous, as the files may be written by Spark 2.x or legacy versions of\nHive, which uses a legacy hybrid calendar that is different from Spark 3.0+'s Proleptic\nGregorian calendar. See more details in SPARK-31404.")
    // Same SparkSession but a different rebase mode for this reader
    sparkSession.read.format("parquet").option("datetimeRebaseMode", "LEGACY")
      .load("/tmp/spark245-input").count() shouldEqual 1
  }
}
