package com.waitingforcode

import org.apache.spark.sql.SparkSession

object JoinHintsExamples extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("Join hints examples").master("local[*]")
    .getOrCreate()

  import testSparkSession.implicits._

  val usersFromShop1 = (0 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop1", "id")
  val usersFromShop2 = (8 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop2", "id")

  def runBroadcastHint(): Unit = {
    val joinedUsersBroadcastHint = usersFromShop1.join(usersFromShop2.hint("BROADCAST"),
      usersFromShop1("id") === usersFromShop2("id"))
    joinedUsersBroadcastHint.explain(true)
  }

  def runShuffleMergeJoinHint(): Unit = {
    val joinedUsersShuffleMergeJoinHint = usersFromShop1.join(usersFromShop2.hint("SHUFFLE_MERGE"),
      usersFromShop1("id") === usersFromShop2("id"))
    joinedUsersShuffleMergeJoinHint.explain(true)
  }

  def runShuffleHashJoinHint(): Unit = {
    val joinedUsersShuffleHashJoinHint = usersFromShop1.join(usersFromShop2.hint("SHUFFLE_HASH"),
      usersFromShop1("id") === usersFromShop2("id"))
    joinedUsersShuffleHashJoinHint.explain(true)
  }

  def runNestedJoinLoopHint(): Unit = {
    val joinedUsersNestedLoopJoinHint = usersFromShop1.join(usersFromShop2.hint("SHUFFLE_REPLICATE_NL"),
      usersFromShop1("id") === usersFromShop2("id"))
    joinedUsersNestedLoopJoinHint.explain(true)
  }

  def runInconsistentJoinHint(): Unit = {
    val joinedUsersInconsistentJoinHint = usersFromShop1.hint("BROADCAST").join(usersFromShop2.hint("SHUFFLE_MERGE"),
      usersFromShop1("id") === usersFromShop2("id"))
    joinedUsersInconsistentJoinHint.explain(true)
  }

  //runBroadcastHint()
  //runShuffleMergeJoinHint()
  //runShuffleHashJoinHint()
  //runNestedJoinLoopHint()
  runInconsistentJoinHint()
}
