package com.waitingforcode

import java.io.File
import java.sql.Timestamp
import java.time.DayOfWeek

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class DateTimeTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: datetime features").master("local[*]")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
  import sparkSession.implicits._

  private val datasetPath = "/tmp/spark/3/datetimetest/input"
  private val datasetLegacyPath = "/tmp/spark/3/datetimetest/input-legacy"
  private val datasetSchema = StructType(Array(
    StructField("datetime_col", TimestampType, false)
  ))
  before {
    val datasetDates = Seq(("2020-01-12 00:00:00")).toDF("datetime_col")
    datasetDates.write.mode(SaveMode.Overwrite).json(datasetPath)

    val datasetDatesLegacy = Seq(("2020-01-35 00:00:00")).toDF("datetime_col")
    datasetDatesLegacy.write.mode(SaveMode.Overwrite).json(datasetLegacyPath)
  }

  after {
    Seq(datasetPath, datasetLegacyPath).foreach(pathToDelete => {
      FileUtils.deleteDirectory(new File(pathToDelete))
    })
  }


  "Proleptic Gregorian Calendar" should "return different date than the hybrid one for the year before its introduction" in {
    val dataset = Seq(("1582-10-12 10:00:00")).toDF("datetime_col")

    val result = dataset.select(
      functions.dayofmonth($"datetime_col"),
      functions.dayofweek($"datetime_col"),
      functions.dayofyear($"datetime_col"),
      functions.year($"datetime_col"),
      functions.month($"datetime_col")
    ).collect()

    result should have size 1
    result(0).getAs[Int]("dayofmonth(datetime_col)") shouldEqual 12
    result(0).getAs[Int]("dayofweek(datetime_col)") shouldEqual 3
    result(0).getAs[Int]("dayofyear(datetime_col)") shouldEqual 285
    result(0).getAs[Int]("year(datetime_col)") shouldEqual 1582
    result(0).getAs[Int]("month(datetime_col)") shouldEqual 10

    // Spark 2 used a hybrid calendar (Julian + Gregorian) and one of the used methods was
    // Timestamp.valueOf("...")
    // As you can see in the test below, the date from my test parsed against this hybrid
    // calendar returns 22 as the day of month
    val hybridCalendarTimestamp = Timestamp.valueOf("1582-10-12 10:00:00")
    hybridCalendarTimestamp.toString shouldEqual "1582-10-22 10:00:00.0"
    val hybridCalendarLocalDateTime = hybridCalendarTimestamp.toLocalDateTime
    hybridCalendarLocalDateTime.getDayOfMonth shouldEqual 22
    hybridCalendarLocalDateTime.getDayOfWeek shouldEqual DayOfWeek.FRIDAY
    hybridCalendarLocalDateTime.getDayOfYear shouldEqual 295
  }

  "not allowed time format characters" should "make the processing fail" in {
    val exception = intercept[IllegalArgumentException] {
      sparkSession.read.option("timestampFormat", "Y A").json(datasetPath).show()
    }

    exception.getMessage should startWith("All week-based patterns are unsupported since Spark 3.0, detected: Y, Please use the SQL function EXTRACT instead")
  }


  "new formatter" should "fail when the whole input is not defined in the pattern" in {
    val jsonData =
      """
        |{"key": "format_date_without_time", "log_action": "2015-07-22 10:00:00" }
      """.stripMargin
    val outputFile = new File("/tmp/test_nanoseconds.json")
    outputFile.deleteOnExit()
    FileUtils.writeStringToFile(outputFile, jsonData)
    val schema = StructType(Seq(
      StructField("key", StringType), StructField("log_action", TimestampType)
    ))

    val readLogs = sparkSession.read.schema(schema)
      .option("timestampFormat", "yyyy-MM")
      .json(outputFile.getAbsolutePath)
      .filter("key IS NOT NULL").collect()
    println(s"=> ${readLogs.mkString(",")}")

//      .map(row => (row.getAs[String]("key"),
      //        row.getAs[Timestamp]("log_action").toString))
    //.show(truncate = false)
//      .collect()
  }


}
