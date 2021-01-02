package com.waitingforcode.bypassmerge

import com.waitingforcode.User
import org.apache.spark.sql.SparkSession

object BypassMergeShuffleWriterExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Bypass merge shuffle writer").master("local[*]")
    .config("spark.default.parallelism", 3)
    // Let's keep it small for a simpler demonstration
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.shuffle.sort.bypassMergeThreshold", 10)
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
