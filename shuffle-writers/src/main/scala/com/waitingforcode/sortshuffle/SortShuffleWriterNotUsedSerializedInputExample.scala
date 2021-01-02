package com.waitingforcode.sortshuffle

import com.waitingforcode.User
import org.apache.spark.sql.SparkSession

object SortShuffleWriterNotUsedSerializedInputExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Sort shuffle writer").master("local[*]")
    // Let's keep it small for a simpler demonstration
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.shuffle.sort.bypassMergeThreshold", 1)
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
