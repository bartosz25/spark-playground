package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ComplexTypesFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. SQL functions").master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      """
        |SELECT
        | ARRAY_SIZE(ARRAY(1, 2, 3, 4, 5)) AS array_length,
        | MAP_CONTAINS_KEY(MAP('a', 'AA', 'b', 'BB'), 'b') AS b_key_in_map
        |""".stripMargin).show(false)
    // Prints
    /*
    +------------+------------+
    |array_length|b_key_in_map|
    +------------+------------+
    |5           |true        |
    +------------+------------+
     */
  }

}
