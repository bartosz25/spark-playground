package com.waitingforcode

import org.apache.spark.sql.SparkSession

/**
 * This demo is here to show that both sides of the join can be demoted. To see that,
 * add breakpoints to the DemoteBroadcastHashJoin's apply if statements with 'hint.leftHint.exists' and
 * 'hint.rightHint.exists'
 */
object AdaptiveQueryExecutionDemoteBroadcastHashJoinBothSides extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Adaptive Query Execution").master("local[*]")
    .config("spark.sql.adaptive.enabled", true)
    .config("spark.sql.autoBroadcastJoinThreshold", "80B")
    .config("spark.sql.adaptive.coalescePartitions.enabled", false)
    .config("spark.sql.adaptive.localShuffleReader.enabled", false)
    .getOrCreate()
  import sparkSession.implicits._

  val input4 = sparkSession.sparkContext.parallelize(
    (1 to 200).map(nr => TestEntryKV(nr, nr.toString)), 100).toDF()
  input4.createOrReplaceTempView("input4")
  val input6 = sparkSession.sparkContext.parallelize(
    (1 to 200).map(nr => TestEntryKV(nr, nr.toString)), 100).toDF()
  input6.createOrReplaceTempView("input6")

  val sqlQuery =
    sparkSession.sql(
      """
        |SELECT * FROM input4 JOIN input6 ON input4.key = input6.key WHERE input4.value = '1' AND input6.value = '1'
      """.stripMargin)

  sqlQuery.collect()

} 