package com.waitingforcode

import org.apache.spark.sql.SparkSession

// #hash-shuffle-join
object ShuffleJoinForFullOuterJoin311 extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("#hash-shuffle-join").master("local[*]")
    // Big number so that shuffle will be more attractive than the broadcast join
    .config("spark.sql.autoBroadcastJoinThreshold", 10000)
    .config("spark.sql.join.preferSortMergeJoin", false)
    .getOrCreate()
  import testSparkSession.implicits._

  val usersFromShop1 = (0 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop1", "id")
  // Only 3 users meaning that the right side is 3 times smaller than the left
  // Because of that, it will become the build side
  val usersFromShop2 = (8 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop2", "id")

  val joinedUsersBuildRight = usersFromShop1.join(usersFromShop2, usersFromShop1("id") === usersFromShop2("id"),
  "full_outer")

  // ShuffledHashJoin [id#8], [id#19], FullOuter, BuildRight
  joinedUsersBuildRight.explain(true)

  val usersFromShop3 = (0 to 35).map(nr => (s"User#${nr}", nr)).toDF("loginShop3", "id")
  val joinedUsersBuildLeft = usersFromShop1.join(usersFromShop3, usersFromShop1("id") === usersFromShop3("id"),
    "full_outer")

  // ShuffledHashJoin [id#8], [id#38], FullOuter, BuildLeft
  joinedUsersBuildLeft.explain(true)
}
