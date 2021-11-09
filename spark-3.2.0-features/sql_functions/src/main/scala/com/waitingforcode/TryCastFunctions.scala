package com.waitingforcode

import org.apache.spark.sql.SparkSession

object TryCastFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Session window demo").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val transformedMaxLong = sparkSession.sql(
      s"""
        |SELECT
        |${Long.MaxValue} + 2 AS long_add_2,
        |TRY_ADD(${Long.MaxValue}, 2) AS long_safe_add_2,
        |CAST(${Long.MaxValue} AS INT) AS long_cast_to_int,
        |TRY_CAST(${Long.MaxValue} AS INT) AS long_safe_cast_to_int,
        |${Long.MaxValue} AS long_max
        |""".stripMargin).as[(Long, Option[Long], Long, Option[Long], Long)].collect().head

    assert(transformedMaxLong == (-9223372036854775807L, None, -1, None, 9223372036854775807L ))
  }

}
