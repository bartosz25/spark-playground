package com.waitingforcode

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NewFunctionsTest extends AnyFlatSpec with Matchers {

  private val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: new Functions").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  "bitwise functions" should "apply to the column values" in {
    Seq(
      (1), (2), (3)
    ).toDF("number").createTempView("bitwise_test")

    val functionsString = sparkSession
      .sql("""
             |SELECT
             |CONCAT(
             | BIT_AND(number), ",", BIT_OR(number), ",", BIT_XOR(number)
             |) AS bitwise_concat
             |FROM bitwise_test""".stripMargin)
      .map(row => row.getAs[String]("bitwise_concat"))
      .collect()

    functionsString should have size 1
    functionsString(0) shouldEqual ("0,3,0")
  }

  "date_part" should "extract day from a timestamp column" in {
    Seq(
      (new Timestamp(0L)), (new Timestamp(TimeUnit.DAYS.toMillis(2L)))
    ).toDF("dt").createTempView("date_part_test")

    val extractedDays = sparkSession
      .sql("SELECT DATE_PART('day', dt) AS extracted_day FROM date_part_test")
      .map(row => row.getAs[Integer]("extracted_day"))
      .collect()

    extractedDays should have size 2
    extractedDays should contain allOf(1, 3)
  }

  "hyperbolic functions" should "return different results than not hyperbolic ones" in {
    Seq(
      (1.3) 
    ).toDF("number").createTempView("hyperbolic_funcs_test")

    val functionsString = sparkSession
      .sql("""
          |SELECT
          |CONCAT(
          | acosh(number), ",", acos(number), ",",
          | asinh(number), ",", asin(number), ",",
          | atanh(number), ",", atan(number)
          |) AS funcs_concat
          |FROM hyperbolic_funcs_test""".stripMargin)
      .map(row => row.getAs[String]("funcs_concat"))
      .collect()

    functionsString should have size 1
    functionsString(0) shouldEqual "0.7564329108569596,NaN,1.078451058954897,NaN,NaN,0.9151007005533605"
  }

  "make_date" should "create a date from 3 separated fields" in {
    Seq(
      (2020, 4, 1),
      (2019, 1, 20),
      (2020, 2, 31) // error on purpose, let's see what happens
    ).toDF("y", "m", "d").createTempView("make_date_test")

    val mappedDates = sparkSession
      .sql("SELECT CAST(make_date(y, m, d) AS STRING) AS date_from_ymd FROM make_date_test")
      .map(row => row.getAs[String]("date_from_ymd"))
      .collect()

    mappedDates should have size 3
    mappedDates should contain allOf(null, "2020-04-01", "2019-01-20")
  }

  "make_timestamp" should "create a timestamp from 4 separated fields" in {
    Seq(
      (2020, 4, 1, 10, 20),
      (2019, 1, 20, 15, 59),
      (2020, 2, 2, 25, 10) // error on purpose, let's see what happens
    ).toDF("y", "m", "d", "h", "min").createTempView("make_timestamp_test")

    val mappedTs = sparkSession
      .sql("SELECT CAST(make_timestamp(y, m, d, h, min, 0, 'UTC') AS STRING) " +
        "AS ts_from_ymd FROM make_timestamp_test")
      .map(row => row.getAs[String]("ts_from_ymd"))
      .collect()

    mappedTs should have size 3
    mappedTs should contain allOf(null, "2020-04-01 12:20:00", "2019-01-20 16:59:00")
  }

  "min_by and max_by" should "retrieve a column for min and max values" in {
    Seq(
      (2020, 4, 1, "year 2020"),
      (2019, 1, 20, "year 2019"),
      (2018, 2, 2, "year 2018")
    ).toDF("y", "m", "d", "label").createTempView("min_by_max_by_test")

    val minAndMaxLabels = sparkSession
      .sql("SELECT min_by(label, y) AS minLabel, max_by(label, y) AS maxLabel FROM min_by_max_by_test")
      .map(row => (row.getAs[String]("minLabel"), row.getAs[String]("maxLabel")))
      .collect()

    minAndMaxLabels should have size 1
    minAndMaxLabels(0) shouldEqual ("year 2018", "year 2020")
  }

  "overlay" should "replace the first 3 letters by xxx" in {
    Seq(
      ("abcdef"),
      ("gh"),
      ("ijk"),
      ("lmn")
    ).toDF("label").createTempView("overlay_test")

    val minAndMaxLabels = sparkSession
      .sql("SELECT OVERLAY(label PLACING 'xxx' FROM 1) AS formattedLabel FROM overlay_test")
      .map(row => row.getAs[String]("formattedLabel"))
      .collect()

    minAndMaxLabels should have size 4
    // As you can see, OVERLAY adds new characters for the columns shorter than 3 characters
    minAndMaxLabels should contain allElementsOf  (Seq("xxxdef", "xxx", "xxx", "xxx"))
  }

  "new aliases" should "do the same thing like existing methods" in {
    Seq(
      (true, false),
      (true, true),
      (true, false)
    ).toDF("bool1", "bool2").createTempView("aliases_test")

    val boolAliases = sparkSession
      .sql(
        """
          |SELECT
          |BOOL_AND(bool1) AS bool_and_version, EVERY(bool1) AS every_version,
          |BOOL_OR(bool2) AS bool_or_version, ANY(bool2) AS any_version
          |FROM aliases_test""".stripMargin)
      .map(row =>
        (row.getAs[Boolean]("bool_and_version"), row.getAs[Boolean]("every_version"),
          row.getAs[Boolean]("bool_or_version"), row.getAs[Boolean]("any_version")))
      .collect()

    boolAliases should have size 1
    boolAliases(0) shouldEqual (true, true, true, true)
  }

  "new aliases for random" should "generate different results every time" in {
    Seq(
      (true, false),
      (true, true),
      (true, false)
    ).toDF("bool1", "bool2").createTempView("random_aliases_test")

    val randomAliases = sparkSession
      .sql(
        """
          |SELECT RANDOM() AS random_v, RAND() AS rand_v
          |FROM random_aliases_test""".stripMargin)
      .map(row =>
        (row.getAs[Double]("random_v"), row.getAs[Double]("rand_v")))
      .collect()

    randomAliases should have size 3
  }
}
