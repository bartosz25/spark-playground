package com.waitingforcode

import org.apache.spark.sql.SparkSession

// #hash-shuffle-join
object JoinGroupByOptimization301 extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("#hash-shuffle-join").master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .config("spark.sql.join.preferSortMergeJoin", false)
    .getOrCreate()
  import testSparkSession.implicits._

  val usersFromShop1 = (0 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop1", "id")
  val usersFromShop2 = (4 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop2", "id")

  val groupedUsers = usersFromShop1.join(usersFromShop2, usersFromShop1("id") === usersFromShop2("id"))
    .groupByKey(row => row.getAs[Int]("id"))
    .count()

  groupedUsers.explain(true)

}
