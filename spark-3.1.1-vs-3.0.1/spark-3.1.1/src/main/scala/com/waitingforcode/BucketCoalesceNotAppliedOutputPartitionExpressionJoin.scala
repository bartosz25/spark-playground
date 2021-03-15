package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object BucketCoalesceNotAppliedOutputPartitionExpressionJoin extends App {

  val lessonWarehouseDir = "/tmp/wfc/spark-3.1.1/bucket-coalesce-not-applied"
  FileUtils.deleteDirectory(new File(lessonWarehouseDir))
  val sparkSession = SparkSession.builder()
    .appName("#coalesce-buckets").master("local[*]").enableHiveSupport()
    .config("spark.sql.warehouse.dir", lessonWarehouseDir)
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .config("spark.sql.bucketing.coalesceBucketsInJoin.enabled", true)
    .getOrCreate()
  import sparkSession.implicits._


  val usersFromShop1 = (0 to 10).map(nr => (s"User#${nr}", nr, nr)).toDF("loginShop1", "id1", "id2")
  usersFromShop1.write.bucketBy(2, "id1", "id2")
    .mode("overwrite").saveAsTable("users_shop_10")
  val usersFromShop2 = (4 to 20).map(nr => (s"User#${nr}", nr, nr)).toDF("loginShop2", "id1", "id2")
  usersFromShop2.write.bucketBy(4, "id1", "id2")
    .mode("overwrite").saveAsTable("users_shop_20")

  sparkSession.sql(
    """SELECT s1.*, s2.* FROM users_shop_10 s1
      |JOIN users_shop_20 s2 ON s2.id1 = s1.id1""".stripMargin)
    .explain(true)

}
