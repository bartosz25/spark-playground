package com.waitingforcode

import org.apache.spark.sql.SparkSession

object PivotExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Pivot example").master("local[*]")
    // Set this to see how Apache Spark prevents the OOM that could be caused
    // by too many distinct pivoted values:
    //.config("spark.sql.pivotMaxValues", 2)
    .getOrCreate()
  import sparkSession.implicits._

  val teams = Seq(
    Team("team1", "France", 3), Team("team2", "Poland", 4), Team("team3", "Germany", 8),
    Team("team4", "France", 3), Team("team5", "Poland", 5), Team("team6", "Germany", 9),
    Team("team7", "France", 3), Team("team5", "Poland", 6), Team("team3", "Germany", 1),
    Team("team1", "France", 3), Team("team1", "Poland", 7), Team("team6", "Germany", 2)
  ).toDF()

  val pivoted = teams.groupBy("country").pivot("name").sum("points")
  pivoted.show(false)

  //println("Pivoted data execution plans")
  //pivoted.explain(true)
}

object PivotExampleForNotAtomicAggregation extends App {

  val sparkSession = SparkSession.builder()
    .appName("Pivot example").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val teams = Seq(
    TeamStringPoints("team1", "France", "3"), TeamStringPoints("team2", "Poland", "4"),
    TeamStringPoints("team3", "Germany", "8"),  TeamStringPoints("team4", "France", "3"),
    TeamStringPoints("team5", "Poland", "5"), TeamStringPoints("team6", "Germany", "9"),
    TeamStringPoints("team7", "France", "3"), TeamStringPoints("team5", "Poland", "6"),
    TeamStringPoints("team3", "Germany", "1"), TeamStringPoints("team1", "France", "3"),
    TeamStringPoints("team1", "Poland", "7"), TeamStringPoints("team6", "Germany", "2")
  ).toDF()

  val pivoted = teams.groupBy("country").pivot("name").agg("points" -> "first")
  pivoted.show(false)
  //pivoted.explain(true)

}

case class Team(name: String, country: String, points: Int)

case class TeamStringPoints(name: String, country: String, points: String)

