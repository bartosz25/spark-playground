package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class RemoveRedundantAggregatesTest extends AnyFlatSpec  {

  // It's a test without assertions but with print, so that it can be run
  // with Maven profiles to check the executed plans
  it should "run remove redundant aggregates optimization" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-33122 : remove redundant aggregates").master("local[*]")
      .config("spark.sql.adaptive.enabled", false)
      .getOrCreate()

    import sparkSession.implicits._
    Seq(
      (1, "a", "A"),
      (1, "a", "A"),
      (2, "b", "B"),
    ).toDF("number", "lower_letter", "upper_letter").createTempView("test_table")

    sparkSession.sql(
      """
        |SELECT number FROM (
        | SELECT number, COUNT(*) FROM test_table GROUP BY number
        |) GROUP BY number
        |""".stripMargin).explain(false)
  }

}
