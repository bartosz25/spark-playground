package com.waitingforcode

import org.apache.spark.sql.SparkSession

object BucketingJoinWithPartitioningExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Bucketed joins").master("local[*]").enableHiveSupport()
    .config("spark.sql.warehouse.dir", "/tmp/spark/buckets-partitions")
    .getOrCreate()
  import sparkSession.implicits._

  val tableName = s"orders2"
  val tableName2 = s"orders2_2"
  val orders = Seq((1L, 1, "user1"), (2L, 1, "user2"), (3L, 2, "user3"), (4L, 3, "user1")).toDF("order_id", "day", "user_id")

//  orders.write.mode("overwrite").partitionBy("day").bucketBy(2, "user_id").saveAsTable(tableName)
  //  orders.write.mode("overwrite").partitionBy("day").bucketBy(2, "user_id").saveAsTable(tableName2)

  sparkSession.sql(s"SELECT * FROM ${tableName} t JOIN " +
    s"${tableName2} t2 ON t.user_id = t2.user_id").explain(true)
}
