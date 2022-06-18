package com.waitingforcode

import org.apache.spark.sql.SparkSession

object AnsiAggregationFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. SQL functions").master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      """
        |SELECT
        | ARRAY_AGG(DISTINCT col) AS distinct_numbers
        | FROM VALUES (1), (2), (2), (3) AS tab(col)
        |""".stripMargin).show(false)
    // Prints
    /*
    +----------------+
    |distinct_numbers|
    +----------------+
    |[1, 2, 3]       |
    +----------------+
     */

    sparkSession.sql(
      """
        |SELECT
        | PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY col) AS disc0_5,
        | PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY col) AS disc0_75,
        | PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col) AS cont0_75,
        | PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY col) AS cont0_75
        | FROM VALUES (10), (20), (30) AS tab(col)
        |""".stripMargin).show(false)
    // Prints
    /*
    +-------+--------+--------+--------+
    |disc0_5|disc0_75|cont0_75|cont0_75|
    +-------+--------+--------+--------+
    |20.0   |10.0    |20.0    |15.0    |
    +-------+--------+--------+--------+
     */


    sparkSession.sql(
      """
        |SELECT
        | REGR_COUNT(x, y),
        | REGR_AVGY(x, y)
        | FROM VALUES (10, NULL), (20, 200), (NULL, 300), (40, 400) AS tab(x, y)
        |""".stripMargin).show(false)
    // Prints
    /*
    +----------------+---------------+
    |regr_count(x, y)|regr_avgy(x, y)|
    +----------------+---------------+
    |2               |30.0           |
    +----------------+---------------+
     */


  }

}
