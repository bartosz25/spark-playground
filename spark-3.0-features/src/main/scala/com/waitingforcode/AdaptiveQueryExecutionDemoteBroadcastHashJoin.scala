package com.waitingforcode

import org.apache.spark.sql.SparkSession

object AdaptiveQueryExecutionDemoteBroadcastHashJoin extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Adaptive Query Execution").master("local[*]")
    .config("spark.sql.adaptive.enabled", true)
    .config("spark.sql.autoBroadcastJoinThreshold", "80B")
    .config("spark.sql.adaptive.coalescePartitions.enabled", false)
    .config("spark.sql.adaptive.localShuffleReader.enabled", false)
    // Set up a very small ratio - normally it's equal to 0.005 - to see the
    // demote not working
    //.config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", 0.00000001d)
    //
    // Uncomment this line if you want to see one side of the join (input5) broadcasted
    // Normally the ratio between non empty and all partitions is 0.015 in that case and
    // setting 0.01 should invalidate the broadcast on the input4 and accept it on the input5
    //.config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", 0.01)
    //
    .getOrCreate()
  import sparkSession.implicits._

  val input4 = sparkSession.sparkContext.parallelize(
    (1 to 200).map(nr => TestEntryKV(nr, nr.toString)), 100).toDF()
  input4.createOrReplaceTempView("input4")
  val input5 = sparkSession.sparkContext.parallelize(Seq(
    TestEntryKV(1, "1"), TestEntryKV(1, "2"),
    TestEntryKV(2, "1"), TestEntryKV(2, "2"),
    TestEntryKV(3, "1"), TestEntryKV(3, "2")
  ), 2).toDF()
  input5.createOrReplaceTempView("input5")

  val sqlQuery =
    sparkSession.sql(
      """
        |SELECT * FROM input4 JOIN input5 ON input4.key = input5.key WHERE input4.value = '1'
      """.stripMargin)

  sqlQuery.collect()

}
