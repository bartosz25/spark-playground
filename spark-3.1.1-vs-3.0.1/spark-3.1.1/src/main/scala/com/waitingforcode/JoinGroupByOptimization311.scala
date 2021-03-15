package com.waitingforcode

import org.apache.spark.sql.SparkSession

// #hash-shuffle-join
object JoinGroupByOptimization311 extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("#hash-shuffle-join").master("local[*]")
    // Big number so that shuffle will be more attractive than the broadcast join
    // left side plan is 352 ; condition: shuffle partitions * threshold
    .config("spark.sql.autoBroadcastJoinThreshold", 2)
    .config("spark.sql.join.preferSortMergeJoin", false)
    .getOrCreate()
  import testSparkSession.implicits._

  val usersFromShop1 = (8 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop1", "id")
  val usersFromShop2 = (0 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop2", "id")

  val groupedUsers = usersFromShop1.join(usersFromShop2,
    usersFromShop1("id") === usersFromShop2("id"))
    .groupBy(usersFromShop1("id"))
    .count()

  groupedUsers.explain(true)

}
