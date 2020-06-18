package com.waitingforcode

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateTimeBackwardCompatibilityTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val datasetLegacyPath = "/tmp/spark/3/datetimetest/input-legacy"
  private val datasetSchema = StructType(Array(
    StructField("datetime_col", TimestampType, false)
  ))

  "EXCEPTION timeParsePolicy" should "make the processing fail for a date that can be parsed with" +
    "old formatter and not the new one" in {
    val testSparkSession = createSparkSession(LegacyBehaviorPolicy.EXCEPTION)

    val sparkUpgradeException = intercept[SparkException] {
      testSparkSession.read.schema(datasetSchema).option("timestampFormat", "yyyy-MM-dd").json(datasetLegacyPath).collect()
    }

    sparkUpgradeException.getMessage contains("You may get a different result due to the upgrading of Spark 3.0")
    sparkUpgradeException.getMessage contains("Fail to parse '2020-01-35 00:00:00' in the new parser. " +
      "You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, " +
      "or set to CORRECTED and treat it as an invalid datetime string.")
  }

  "LEGACY timeParsePolicy" should "fallback to the legacy parser and restore old parsing behavior" in {
    val testSparkSession = createSparkSession(LegacyBehaviorPolicy.LEGACY)
    import testSparkSession.implicits._

    val timestamps = testSparkSession.read.schema(datasetSchema).option("timestampFormat", "yyyy-MM-dd")
      .json(datasetLegacyPath).as[String].collect()

    timestamps should have size 1
    timestamps(0) shouldEqual "2020-02-04 00:00:00"
  }

  "CORRECTED timeParsePolicy" should "consider parsing error as a missing value" in {
    val testSparkSession = createSparkSession(LegacyBehaviorPolicy.CORRECTED)
    import testSparkSession.implicits._

    val timestamps = testSparkSession.read.schema(datasetSchema)
      .json(datasetLegacyPath).as[String].collect()

    timestamps should have size 1
    timestamps(0) shouldBe null
  }

  private def createSparkSession(policy: LegacyBehaviorPolicy.Value): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.0: datetime features").master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", policy.toString)
      .getOrCreate()
    import sparkSession.implicits._

    val datasetDatesLegacy = Seq(("2020-01-35 00:00:00")).toDF("datetime_col")
    datasetDatesLegacy.write.mode(SaveMode.Overwrite).json(datasetLegacyPath)

    sparkSession
  }

}
