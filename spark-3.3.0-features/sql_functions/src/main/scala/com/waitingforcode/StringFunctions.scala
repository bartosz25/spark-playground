package com.waitingforcode

import org.apache.spark.sql.SparkSession

object StringFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. SQL functions").master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      """
        |SELECT
        | CONTAINS('defabcxyz', 'abc') AS contains_abc,
        | STARTSWITH('abcdefxyz', 'abc') AS startswith_abc,
        | ENDSWITH('defxyzabc', 'abc') AS endswith_abc,
        | 'defxyzABC' LIKE '%abc' AS case_sensitive_like,
        | 'defxyzABC' ILIKE '%abc' AS case_insensitive_like
        |""".stripMargin).show(false)
    // Prints
    /*
    +------------+--------------+------------+-------------------+---------------------+
    |contains_abc|startswith_abc|endswith_abc|case_sensitive_like|case_insensitive_like|
    +------------+--------------+------------+-------------------+---------------------+
    |true        |true          |true        |false              |true                 |
    +------------+--------------+------------+-------------------+---------------------+
     */

  }

}
