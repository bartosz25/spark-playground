package com.waitingforcode

import org.apache.spark.sql.SparkSession

object UsersAggregator extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark Shuffle writers").master("local[*]")
    // Let's keep it small for a simpler demonstration
    .config("spark.sql.shuffle.partitions", 5)
    .getOrCreate()

  import sparkSession.implicits._
  val usersToGroup = (0 to 10).map(id => User(s"id#${id}", s"login${id}"))
    .toDS()

  val groupedUsers = usersToGroup.groupByKey(user => user.id)
    .flatMapGroups((userId, usersGroup) => {
      Seq(s"${usersGroup.mkString(",")}")
    })

  groupedUsers.show(truncate = false)
}

case class User(id: String, login: String)
