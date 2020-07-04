package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class PostgreSQLCompatibilitySyntaxChangesTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val syntaxChangeSparkSession = SparkSession.builder()
    .appName("Spark 3.0 features - syntax changes test").master("local[*]").enableHiveSupport()
    .config("spark.sql.ansi.enabled", true)
    .getOrCreate()
  import syntaxChangeSparkSession.implicits._

  private val viewName = "syntax_changes"

  before {
    syntaxChangeSparkSession.sql(s"DROP TABLE IF EXISTS ${viewName}")
    val dataset = Seq(
      (0, "a", true, "aaa"), (1, "b", false, "bbb"), (2, "c", true, "ccc")
    ).toDF("nr", "letter", "bool_flag_view", "code")
    dataset.createOrReplaceTempView(viewName)
  }
  after {
    syntaxChangeSparkSession.sql(s"DROP TABLE IF EXISTS ${viewName}")
  }

  "FILTER WHERE clause" should "keep only the rows matching the predicate in the aggregate" in {
    val counters = syntaxChangeSparkSession.sql(
      s"""
         |SELECT
         |COUNT(*) AS all_elements,
         |COUNT(*) FILTER (WHERE nr < 1) AS matching_elements
         |FROM ${viewName}
         |""".stripMargin).as[(Long, Long)].collect()

    counters should have size 1
    counters(0) shouldEqual (3, 1)
  }

  "WITH clause" should "be supported in subquery" in {
    val numbers = syntaxChangeSparkSession.sql(
      """
        |SELECT * FROM (
        | WITH numbers(number) AS (
        |   (SELECT 1) UNION (SELECT 2)
        | )
        | SELECT number FROM numbers
        |)
        |""".stripMargin).as[Int].collect()

    numbers should have size 2
    numbers should contain allOf(1, 2)
  }

  "implementation-dependent changes" should "enable new SQL syntax" in {
    val transformed = syntaxChangeSparkSession.sql(
      """
        |SELECT
        | CAST('yes' AS BOOLEAN) AS boolFlagTrue, CAST('no' AS BOOLEAN) AS boolFlagFalse,
        | SUBSTRING('1234567' FROM 3 FOR 2) AS substringText,
        | LPAD('abc', 5, '.') AS lpadChars,
        | LPAD('abc', 5) AS lpadDefault,
        | TRIM('abc' FROM 'aXXXXcYYYb') AS customTrim,
        | TRIM(TRAILING FROM '   abc    ') AS positionalTrim
        |""".stripMargin
    ).as[ImplementationDependentQueryResult].collect()

    transformed should have size 1
    transformed(0) shouldEqual ImplementationDependentQueryResult(
      boolFlagTrue = true, boolFlagFalse = false, substringText = "34",
      lpadChars = "..abc", lpadDefault =  "  abc", customTrim = "XXXXcYYY",
      positionalTrim = "   abc"
    )
  }

  "new ANSI-compliant syntax changes" should "be included in Spark 3.0" in {
    val testTableName = "data_types_table"
    // First change, real data type is an alias for the float and numeric of decimal
    // https://github.com/apache/spark/pull/26537/files
    // But only for the queries, there is no new RealType nor NumericType -> Float and Decimal are kept
    syntaxChangeSparkSession.sql(s"DROP TABLE IF EXISTS ${testTableName}")
    syntaxChangeSparkSession.sql(
      s"""
         |CREATE TABLE ${testTableName} (average_salary REAL, mean_salary NUMERIC(5, 2))
         |""".stripMargin)

    // Another interesting feature, the nested bracket comments
    syntaxChangeSparkSession.sql(
      s"""
         |SELECT *
         |/* commented now, we take all cols every time
         |   SELECT nr, letter
         |*/ FROM ${viewName}
         |""".stripMargin).show()

    // A new boolean predicate syntax is also supported
    val matchingNumbers = syntaxChangeSparkSession.sql(
      s"""
         |SELECT nr FROM ${viewName} WHERE bool_flag_view IS TRUE
         |""".stripMargin).as[Long].collect()
    matchingNumbers should have size 2
    matchingNumbers should contain allOf(0, 2)

    // New alias for decimal fields
    val schemaFromCast = syntaxChangeSparkSession.sql(
      """
        |SELECT CAST('2.0' AS DEC) AS casted_column
        |""".stripMargin).schema
    schemaFromCast shouldEqual StructType(Array(StructField("casted_column", DecimalType(10, 0), true)))
  }



}


case class ImplementationDependentQueryResult(boolFlagTrue: Boolean, boolFlagFalse: Boolean,
                                              substringText: String, lpadChars: String, lpadDefault: String,
                                              customTrim: String, positionalTrim: String)

