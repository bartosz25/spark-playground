package com.waitingforcode

import org.apache.spark.sql.SparkSession

object DateTimeFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. SQL functions").master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      """
        |SELECT
        | TIMESTAMPADD(HOUR, 2, TIMESTAMP '2022-06-01 20:00:00') AS eight_pm_in_2_hours,
        | DATEADD(HOUR, 45, DATE '2022-06-01 10:00:00') AS date_in_2_days,
        | DATEADD(HOUR, 48, DATE '2022-06-01 10:00:00') AS date_in_3_days
        |""".stripMargin)
      .show(false)

    // Prints:
    /*
    +-------------------+-------------------+-------------------+
    |eight_pm_in_2_hours|date_in_2_days     |date_in_3_days     |
    +-------------------+-------------------+-------------------+
    |2022-06-01 22:00:00|2022-06-02 21:00:00|2022-06-03 00:00:00|
    +-------------------+-------------------+-------------------+
     */


    sparkSession.sql(
      """
        |SELECT
        | TIMESTAMPDIFF(MINUTE, TIMESTAMP '2022-06-01 20:00:00',
        |     TIMESTAMP '2022-06-01 20:40:00') AS tms_diff,
        | DATEDIFF(MINUTE, DATE '2022-06-01', DATE '2022-06-02') AS date_diff
        |""".stripMargin)
      .show(false)
    // Prints:
    /*
    +--------+---------+
    |tms_diff|date_diff|
    +--------+---------+
    |40      |1440     |
    +--------+---------+
     */

  }
}
