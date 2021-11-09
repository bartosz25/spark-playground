package com.waitingforcode

import org.apache.spark.sql.SparkSession

object GroupingSetsFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Session window demo").master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    Seq(
      ("Europe", "Paris", "capital", "FR", 2138551),
      ("Europe", "Marseille", "not capital", "FR", 794811),
      ("Europe", "Warsaw", "capital",  "PL", 1702139),
      ("Europe", "Lodz", "not capital", "PL", 768755),
      ("North America", "New York", "not capital","US", 8175133),
      ("North America", "Los Angeles", "not capital", "US", 3971883),
    ).toDF("continent", "city", "city_role", "country", "population").createTempView("test_table")


    // Feature 1 - ordinals support
    sparkSession.sql(
      """
        |SELECT city, country, SUM(population) AS global_population
        |FROM test_table
        |GROUP BY CUBE(1, 2)
        |""".stripMargin).show(false)

    // Feature 2 - static grouping column
    sparkSession.sql(
      """
        |SELECT continent, city, country, SUM(population) AS global_population
        |FROM test_table
        |GROUP BY continent, CUBE(city, country)
        |""".stripMargin).show(false)

    println("===================== ROLLUP ===========================")
    sparkSession.sql(
      """
        |SELECT city, country, SUM(population) AS global_population
        |FROM test_table
        |GROUP BY ROLLUP(city, country)
        |""".stripMargin).show(200, false)
    println("=====================   CUBE ===========================")
    sparkSession.sql(
      """
        |SELECT continent, country, SUM(population) AS global_population
        |FROM test_table
        |GROUP BY CUBE(continent, country)
        |""".stripMargin).show(200, false)
    println("=====================   BOTH ===========================")

    Seq(
      ("a", "b", "c", "d", 1),
      ("a", "b", "c", "d", 1),
      ("A", "B", "C", "D", 1),
    ).toDF("a", "b", "c", "d", "v").createTempView("test_table_letters")

    sparkSession.sql(
      """
        |SELECT a, b, c, d, SUM(v) AS sum_value
        |FROM test_table_letters
        |GROUP BY ROLLUP(a, b), CUBE(c, d)
        |""".stripMargin).show(200, false)
  }

}
