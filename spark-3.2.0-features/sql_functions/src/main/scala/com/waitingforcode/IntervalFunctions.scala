package com.waitingforcode

import org.apache.spark.sql.SparkSession

import java.time.{Duration, Instant, Period}

object IntervalFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Session window demo").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val interval = sparkSession.sql(
      """
        |SELECT
        |MAKE_DT_INTERVAL(1, 3) AS datetime_interval_value,
        |MAKE_YM_INTERVAL(2021, 3) AS year_month_interval_value
        |""".stripMargin)
      // Under-the-hood, they can be converted to Duration and Period
      // Check: https://issues.apache.org/jira/browse/SPARK-34615 and https://issues.apache.org/jira/browse/SPARK-34605
      .as[(Duration, Period)].collect().head
    assert(interval._1.toMillis == Instant.parse("1970-01-02T03:00:00Z").toEpochMilli)
    assert(interval._2.getMonths == 3)
    assert(interval._2.getYears == 2021)
  }

}
