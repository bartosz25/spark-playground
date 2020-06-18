package com.waitingforcode

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateTimeRebaseTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val datasetLegacyPath = "/tmp/spark/3/datetimetest/input-legacy-parquet"
  private val datasetSchema = StructType(Array(
    StructField("datetime_col", TimestampType, false)
  ))
  private val inputPath = "/home/bartosz/workspace/spark-playground/spark-3.0-features/src/test/resources/parquet-spark-2.4.0"

  "file created by Spark 2.4 read with CORRECTED mode" should "return invalid date" in {
    // FYI, in `corrected` we read the dates as it, so without rebasing
    val testSparkSession = createSparkSession(LegacyBehaviorPolicy.CORRECTED)
    import testSparkSession.implicits._

    val dateFromSpark240 = testSparkSession.read.parquet(s"${inputPath}").as[String].collect()

    dateFromSpark240 should have size 1
    // 1000-01-06 09:09:21 => but the initial date was 1000-01-01 10:00:00
    dateFromSpark240(0) shouldEqual "1000-01-06 09:09:21"
  }

  "file created by Spark 2.4 read with EXCEPTION mode" should "fail for ambiguous dates" in {
    val testSparkSession = createSparkSession(LegacyBehaviorPolicy.EXCEPTION)
    import testSparkSession.implicits._

    val sparkException = intercept[SparkException] {
      testSparkSession.read.parquet(s"${inputPath}").as[String].collect()
    }

    sparkException.getMessage() should include("SparkUpgradeException: You may get a different result due " +
      "to the upgrading of Spark 3.0")
    sparkException.getMessage() should include("reading dates before 1582-10-15 or " +
      "timestamps before 1900-01-01T00:00:00Z from Parquet files can be ambiguous, " +
      "as the files may be written by Spark 2.x or legacy versions of Hive, " +
      "which uses a legacy hybrid calendar that " +
      "is different from Spark 3.0+'s Proleptic Gregorian calendar")
  }

  "file created by Spark 2.4 read with LEGACY mode" should "return valid date" in {
    val testSparkSession = createSparkSession(LegacyBehaviorPolicy.LEGACY)
    import testSparkSession.implicits._

    val dateFromSpark240 = testSparkSession.read.parquet(s"${inputPath}").as[String].collect()

    dateFromSpark240 should have size 1
    // 1000-01-06 09:09:21 => but the initial date was 1000-01-01 10:00:00
    dateFromSpark240(0) shouldEqual "1000-01-01 10:00:00"
  }

  private def createSparkSession(policy: LegacyBehaviorPolicy.Value): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.0: datetime features").master("local[*]")
      .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", policy.toString)
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", policy.toString)
      .getOrCreate()
    sparkSession
  }
}
