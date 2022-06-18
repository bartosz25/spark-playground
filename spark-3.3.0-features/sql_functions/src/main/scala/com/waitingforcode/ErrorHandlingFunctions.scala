package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ErrorHandlingFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. SQL functions").master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      s"""
         |SELECT
         |${Long.MaxValue} * 2 AS not_safe_multiply,
         |TRY_MULTIPLY(${Long.MaxValue}, 2) AS long_safe_multiply
         |""".stripMargin)
      .show(false)
    // Prints
    /*
    +-----------------+------------------+
    |not_safe_multiply|long_safe_multiply|
    +-----------------+------------------+
    |-2               |null              |
    +-----------------+------------------+
     */

    sparkSession.sql(
      s"""
         |SELECT
         |TRY_SUM(data),
         |SUM(data)
         |FROM VALUES (${Long.MaxValue}), (${Long.MaxValue}) AS test_table(data)
         |""".stripMargin)
      .show(false)
    // Prints
    /*
    +-------------+---------+
    |try_sum(data)|sum(data)|
    +-------------+---------+
    |null         |-2       |
    +-------------+---------+
     */
  }

}
