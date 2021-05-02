package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}

class NewFunctionsTest extends AnyFlatSpec with Matchers {

  private val sparkSession = SparkSession.builder()
    .appName("Spark 3.1: new functions").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  "json_object_keys and json_array_length" should "return correct results as new Apache Spark 3.1.1 functions" in {
    Seq(
      ("""{"number1": 1, "number2": {"number3": 3}, "number4": 4, "number5": [5, 5, 5]}"""),
      ("""{"number1": 1, "number2": {"number3": 3}, "number4": 4, "number5": [5, 5, 5, 5, 5, 5], "number6": 6}""")
    ).toDF("json_string").createTempView("json_fields_table")

    // TODO: upper case for all the functions!
    val response = sparkSession.sql(
      """
        |SELECT
        | json_object_keys(json_string) AS outer_fields,
        | json_array_length(get_json_object(json_string, "$.number5")) AS number5_length
        |FROM json_fields_table
        |""".stripMargin).as[(Seq[String], Int)].collect()

    response should have length 2
    response should contain allOf(
      (Seq("number1", "number2", "number4", "number5"), 3),
      (Seq("number1", "number2", "number4", "number5", "number6"), 6)
    )
  }

  "new datetime functions" should "return correct results" in {
    Seq(
      (Timestamp.valueOf("1970-01-01 04:00:52.123456789"), Date.valueOf("1970-01-20"))
    ).toDF("datetime_column", "date_column").createTempView("datetime_functions_table")

    sparkSession.sql(
      """
        |SELECT
        | unix_seconds(datetime_column) AS unix_seconds_value,
        | unix_millis(datetime_column) AS unix_milliseconds_value,
        | unix_micros(datetime_column) AS unix_microseconds_value,
        | timestamp_seconds(1) AS timestamp_seconds_value,
        | timestamp_millis(1000) AS timestamp_millis_value,
        | timestamp_micros(1000000) AS timestamp_micros_value,
        | date_from_unix_date(30) AS date_from_unix_date_value,
        | unix_date(date_column) AS unix_date_value,
        | current_timezone() AS my_timezone
        |FROM datetime_functions_table
        |""".stripMargin).show(false)
    // TODO: unix_date_value = 19 WHY Not 20?, date_from_unix_date(30) = 1970-01-31; WHY not 1970-01-30?

    SQLConf.get.sessionLocalTimeZone
  }

  "metadata functions" should "return correct results" in {
    Seq(
      ("A")
    ).toDF("letter").createTempView("letters")

    sparkSession.sql(
      """
        |SELECT
        | WIDTH_BUCKET(6, 1, 10, 5) AS bucket_for_5_buckets,
        | WIDTH_BUCKET(6, 1, 10, 2) AS bucket_for_2_buckets,
        | CURRENT_CATALOG() AS current_catalog
        |FROM letters
        |""".stripMargin).show(false)
  }

}
