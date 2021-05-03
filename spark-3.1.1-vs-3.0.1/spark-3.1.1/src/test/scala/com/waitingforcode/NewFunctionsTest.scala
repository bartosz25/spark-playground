package com.waitingforcode

import org.apache.spark.SparkException
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class NewFunctionsTest extends AnyFlatSpec with Matchers {

  private val sparkSession = SparkSession.builder()
    .appName("Spark 3.1: new functions").master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
  import sparkSession.implicits._

  "json_object_keys and json_array_length" should "return correct results as new Apache Spark 3.1.1 functions" in {
    Seq(
      ("""{"number1": 1, "number2": {"number3": 3}, "number4": 4, "number5": [5, 5, 5]}"""),
      ("""{"number1": 1, "number2": {"number3": 3}, "number4": 4, "number5": [5, 5, 5, 5, 5, 5], "number6": 6}""")
    ).toDF("json_string").createTempView("json_fields_table")

    val response = sparkSession.sql(
      """
        |SELECT
        | JSON_OBJECT_KEYS(json_string) AS outer_fields,
        | JSON_OBJECT_KEYS(GET_JSON_OBJECT(json_string, "$.number2")) AS nested_keys,
        | JSON_ARRAY_LENGTH(GET_JSON_OBJECT(json_string, "$.number5")) AS number5_length
        |FROM json_fields_table
        |""".stripMargin).as[(Seq[String], Seq[String], Int)].collect()

    response should have length 2
    response should contain allOf(
      (Seq("number1", "number2", "number4", "number5"), Seq("number3"), 3),
      (Seq("number1", "number2", "number4", "number5", "number6"), Seq("number3"), 6)
    )
  }

  "integers" should "be converted to timestamps with new functions" in {
    val timestampsFromInts = sparkSession.sql(
      """
        |SELECT
        | TIMESTAMP_SECONDS(1) AS timestamp_seconds_value,
        | TIMESTAMP_MILLIS(1000) AS timestamp_millis_value,
        | TIMESTAMP_MICROS(1000000) AS timestamp_micros_value
        |""".stripMargin).as[(String, String, String)].collect()

    timestampsFromInts should have size 1
    timestampsFromInts(0) shouldEqual ("1970-01-01 01:00:01", "1970-01-01 01:00:01", "1970-01-01 01:00:01")
  }

  "timestamps" should "be converted back to integers with new functions" in {
    Seq(
      (Timestamp.valueOf("1970-01-01 01:00:01.000000000"))
    ).toDF("timestamp_column").createTempView("datetime_functions_table")

    val integersFromTimestamps = sparkSession.sql(
      """
        |SELECT
        | UNIX_SECONDS(timestamp_column) AS unix_seconds_value,
        | UNIX_MILLIS(timestamp_column) AS unix_milliseconds_value,
        | UNIX_MICROS(timestamp_column) AS unix_microseconds_value
        |FROM datetime_functions_table
        |""".stripMargin).as[(Long, Long, Long)].collect()

    integersFromTimestamps should have size 1
    integersFromTimestamps(0) shouldEqual (1L, 1000L, 1000000L)
  }

  "integers" should "be converted to dates with the new functions" in {
    val dateConversionResults = sparkSession.sql(
      """
        |SELECT
        | DATE_FROM_UNIX_DATE(0) AS date_from_unix_date_day_0,
        | DATE_FROM_UNIX_DATE(30) AS date_from_unix_date_day_30,
        | UNIX_DATE(DATE_FROM_UNIX_DATE(30)) AS unix_date_value_day_30
        |""".stripMargin).as[(String, String, String)].collect()

    dateConversionResults should have size 1
    dateConversionResults(0) shouldEqual (("1970-01-01", "1970-01-31", "30"))
  }

  "current_timezone()" should "return the timezone of SparkSession" in {
    val currentTimezone = sparkSession.sql(
      """
        |SELECT CURRENT_TIMEZONE()
        |""".stripMargin).as[String].collect()

    currentTimezone should have size 1
    currentTimezone(0) shouldEqual "UTC"
  }

  "metadata functions" should "return correct results" in {
    val bucketsWithCatalog = sparkSession.sql(
      """
        |SELECT
        | WIDTH_BUCKET(6, 1, 10, 5) AS bucket_for_5_buckets,
        | WIDTH_BUCKET(6, 1, 10, 2) AS bucket_for_2_buckets,
        | CURRENT_CATALOG() AS current_catalog
        |""".stripMargin).as[(BigInt, BigInt, String)].collect()

    bucketsWithCatalog should have size 1
    bucketsWithCatalog(0) shouldEqual((3, 2, "spark_catalog"))
  }

  "nth_value" should "return arbitrary values for every window" in {
    val playersDataset = Seq(
      ("player1", Some(1), 1), ("player2", Some(1), 1),
      ("player1", Some(5), 2), ("player2", Some(3), 2),
      ("player1", Some(11), 3), ("player2", None, 3),
      ("player1", Some(19), 4), ("player2", Some(7), 4)
    ).toDF("player_login", "new_points", "timestamp")

    val playerWindow = Window.partitionBy($"player_login").orderBy($"timestamp".asc)
    val thirdPointDoesntSkipNulls = functions.nth_value($"new_points", 3, ignoreNulls = false).over(playerWindow)
    val thirdPointSkipsNulls = functions.nth_value($"new_points", 3, ignoreNulls = true).over(playerWindow)

    val playersPointIn3rdPosition = playersDataset
      .select(functions.concat($"player_login", functions.lit("-"), $"timestamp"),
        thirdPointDoesntSkipNulls.as("point_not_ignore_nulls"),
        thirdPointSkipsNulls.as("point_ignore_nulls"))
      .as[(String, Option[Int], Option[Int])]
      .collect()

    playersPointIn3rdPosition should have size 8
    playersPointIn3rdPosition should contain allOf(
      ("player1-1", None, None), ("player1-2", None, None),
      ("player1-3", Some(11), Some(11)), ("player1-4", Some(11), Some(11)),
      ("player2-1", None, None), ("player2-2", None, None),
      // As you can see, with ignoreNulls=true, the nth position ignores nulls
      // and that's why you can see that the 4th row is returned
      ("player2-3", None, None), ("player2-4", None, Some(7))
    )
  }

  "assert_true" should "fail the query when one of the conditions is false" in {
    // Repartition to show that there are more than 1 task
    Seq((4), (8), (10), (12), (3), (6)).toDF("number1").repartition(6)
      .createTempView("error_management_table")

    val exception = intercept[SparkException] {
      sparkSession.sql(
        """
          |SELECT
          | ASSERT_TRUE(number1 % 2 == 0, CONCAT(number1, " is an odd number")),
          | number1
          |FROM error_management_table
          |""".stripMargin).show(false)
    }
    exception.getMessage should include("java.lang.RuntimeException: 3 is an odd number")
  }

  "raise_error" should "raise an error in an if-else statement" in {
    // Repartition to show that there are more than 1 task
    Seq((4), (8), (10), (12), (3), (6)).toDF("number1")
      .repartition(6).createTempView("error_management_table")

    val exception = intercept[SparkException] {
      sparkSession.sql(
        """
          |SELECT
          | number1,
          | CASE WHEN number1 % 2 != 0 THEN RAISE_ERROR(CONCAT(number1, " is an odd number"))
          |   ELSE "The number is even"
          | END
          |FROM error_management_table
          |""".stripMargin).show(false)
    }
    exception.getMessage should include("java.lang.RuntimeException: 3 is an odd number")
  }

  "regexp_extract_all" should "extract all matching patterns" in {
    Seq(("1,2,3 abcdef 4,5,6"))
      .toDF("sample_text").createTempView("regexp_table")

    val extractedExpressions = sparkSession.sql(
      """
        |SELECT
        | REGEXP_EXTRACT_ALL(sample_text, "(\\d+)") AS extraction,
        | REGEXP_EXTRACT_ALL(sample_text, "(\\d+),(\\d+),(\\d+)", 2) AS extraction_groups,
        | REGEXP_EXTRACT(sample_text, "(\\d+)") AS extraction_simple,
        | REGEXP_EXTRACT(sample_text, "(\\d+),(\\d+),(\\d+)", 2) AS extraction_simple_groups
        |FROM regexp_table
        |""".stripMargin).as[(Seq[String], Seq[String], String, String)].collect()

    extractedExpressions should have size 1
    extractedExpressions(0) shouldEqual (
      Seq("1", "2", "3", "4", "5", "6"),
      Seq("2", "5"),
      "1",
      "2"
      )
  }

}
