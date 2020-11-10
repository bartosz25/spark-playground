package com.waitingforcode

import org.apache.spark.sql.SparkSession

object StackExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Stack example").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val pivotedDf = Seq(
    (1, 30, 300, 3000), (2, 50, 500, 5000), (3, 100, 1000, 10000), (4, 200, 2000, 20000)
  ).toDF("id", "team1", "team2", "team3")
  
  pivotedDf.show(false)
  println("Unpivoting")
  pivotedDf.createOrReplaceTempView("pivoted_table")

  val stackedDf = sparkSession.sql(
    "SELECT id, STACK(3, 'team1_new', team1, " +
      "'team2_new', team2, 'team3_new', team3) AS (team, points) " +
      "FROM pivoted_table")

  stackedDf.explain(true)
  stackedDf.show(true)


}
