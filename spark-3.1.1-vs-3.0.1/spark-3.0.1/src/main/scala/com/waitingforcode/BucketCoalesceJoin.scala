package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object BucketCoalesceJoin extends App {
  val lessonWarehouseDir = "/tmp/wfc/spark-3.0.1/bucket-coalesce"
  FileUtils.deleteDirectory(new File(lessonWarehouseDir))
  val sparkSession = SparkSession.builder()
    .appName("#coalesce-buckets").master("local[*]").enableHiveSupport()
    .config("spark.sql.warehouse.dir", lessonWarehouseDir)
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .getOrCreate()
  import sparkSession.implicits._


  val usersFromShop1 = (0 to 10).map(nr => (s"User#${nr}", nr)).toDF("loginShop1", "id")
  usersFromShop1.write.bucketBy(2, "id")
    .mode("overwrite").saveAsTable("users_shop_1")
  val usersFromShop2 = (4 to 20).map(nr => (s"User#${nr}", nr)).toDF("loginShop2", "id")
  usersFromShop2.write.bucketBy(4, "id")
    .mode("overwrite").saveAsTable("users_shop_2")
  val usersFromShop3 = (4 to 20).map(nr => (s"User#${nr}", nr)).toDF("loginShop3", "id")
  usersFromShop3.write.bucketBy(5, "id")
    .mode("overwrite").saveAsTable("users_shop_3")

  println("================== Join with shuffle 1 =====================")
  sparkSession.sql(
    """SELECT s1.*, s2.* FROM users_shop_1 s1
      |JOIN users_shop_2 s2 ON s2.id = s1.id""".stripMargin)
    .explain(true)

  println("================== Join with shuffle 2 =====================")
  sparkSession.sql(
    """SELECT s1.*, s3.* FROM users_shop_1 s1
      |JOIN users_shop_3 s3 ON s3.id = s1.id""".stripMargin)
    .explain(true)
}
