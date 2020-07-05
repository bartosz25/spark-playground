package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PostgreSQLCompatibilityAnsiEnabledTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  behavior of "ANSI enabled flag"

  it should "disallow the use of forbidden keywords when enabled" in {
    val sparkSession = ansiSparkSession(true)

    sparkSession.sql("DROP TABLE IF EXISTS test_ansi_enabled")
    val parseError = intercept[ParseException] {
      // current_user and primary are one of the new reserved keywords
      // let's use them in the schema to make the table creation fail
      sparkSession.sql(
        """
          |CREATE TABLE test_ansi_enabled (current_user STRING, primary BOOLEAN)
          |""".stripMargin)
    }

    parseError.getMessage() should include("no viable alternative at input 'current_user'(line 2, pos 32)")
  }

  it should "allow the use of forbidden keywords when disabled" in {
    val sparkSession = ansiSparkSession(false)

    sparkSession.sql("DROP TABLE IF EXISTS test_ansi_enabled")
    sparkSession.sql(
      """
        |CREATE TABLE test_ansi_enabled (current_user STRING, primary BOOLEAN)
        |""".stripMargin)
    sparkSession.sql("DROP TABLE IF EXISTS test_ansi_enabled")
  }

  def ansiSparkSession(ansiFlag: Boolean) = SparkSession.builder()
    .appName("Spark 3.0 features - ANSI test").master("local[*]").enableHiveSupport()
    .config("spark.sql.ansi.enabled", ansiFlag)
    .getOrCreate()

}