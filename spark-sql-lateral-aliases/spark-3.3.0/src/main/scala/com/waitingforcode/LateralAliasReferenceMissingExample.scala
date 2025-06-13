package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.io.File
import scala.util.{Failure, Try}

object LateralAliasReferenceMissingExample {
  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/lateral-aliases-3.3.0"
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()
    FileUtils.deleteDirectory(new File(outputDir))

    // It works thanks to a lateralAliasReference!
    import sparkSession.implicits._
    Seq(
      ("""{"lower": "a", "upper": "A"}""")
    ).toDF("letter_json").createOrReplaceTempView("letters")
    val error = Try(sparkSession.sql(
      """
        |SELECT
        | FROM_JSON(letter_json, "lower STRING, upper STRING") AS letter,
        | CONCAT_WS(" -> ", letter.lower, letter.upper) AS concatenated
        |FROM letters
        |""".stripMargin).explain(true))
    error match {
      case Failure(e) => e.printStackTrace()
    }

    sparkSession.sql(
      """
        |SELECT CONCAT_WS(" -> ", letter.lower, letter.upper) AS concatenated FROM
        |(
        |  SELECT
        |  FROM_JSON(letter_json, "lower STRING, upper STRING") AS letter
        |  FROM letters
        |) AS l
        |""".stripMargin).explain(true)
  }
}
