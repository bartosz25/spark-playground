package com.waitingforcode

import org.apache.spark.sql.SparkSession

object BucketingJoinExample extends App {

  val  sparkSession = SparkSession.builder()
    .appName("Bucketed joins").master("local[*]").enableHiveSupport()
    .config("spark.sql.adaptive.enabled", false)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()
  import sparkSession.implicits._

  val tableName = s"orders"
  val tableName2 = s"orders_2"
  val orders = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1")).toDF("order_id", "user_id")

  //  orders.write.mode("overwrite").bucketBy(2, "user_id").saveAsTable(tableName)
  //  orders.write.mode("overwrite").bucketBy(2, "user_id").saveAsTable(tableName2)

  sparkSession.sql(s"SELECT * FROM ${tableName} t JOIN " +
    s"${tableName2} t2 ON t.user_id = t2.user_id")
    .collect()
    //.explain(true)
}


object BucketingJoinExampleDiffBucketCount extends App {

  val  sparkSession = SparkSession.builder()
    .appName("Bucketed joins").master("local[*]").enableHiveSupport()
    // .config("spark.sql.warehouse.dir", "/tmp/spark/buckets")
    .config("spark.sql.adaptive.enabled", false)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()
  import sparkSession.implicits._

  val tableName = s"orders_diff_bucket_count"
  val tableName2 = s"orders_diff_bucket_count_2"
  val orders = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1")).toDF("order_id", "user_id")

  orders.write.mode("overwrite").bucketBy(2, "user_id").saveAsTable(tableName)
  orders.write.mode("overwrite").bucketBy(5, "user_id").saveAsTable(tableName2)

  sparkSession.sql(s"SELECT * FROM ${tableName} t JOIN " +
    s"${tableName2} t2 ON t.user_id = t2.user_id")
  //  .collect()
  .explain(true)
}

