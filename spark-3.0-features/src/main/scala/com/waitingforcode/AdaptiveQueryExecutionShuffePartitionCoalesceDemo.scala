package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ShufflePartitionCoalesceConfiguration {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: AQE - shuffle partitions coalesce").master("local[*]")
    .config("spark.sql.adaptive.enabled", true)
    .config("spark.sql.adaptive.logLevel", "TRACE")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "2")
    // Initial number of shuffle partitions
    // I put a very big number of the initial partitions to make the coalescing happen
    // coalesce - about setting the right number of reducer task
    .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "100")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "10KB")
    // Disable broadcast to be sure that the plan has at least one shuffle
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.join.preferSortMergeJoin", true)
    .getOrCreate()
  import sparkSession.implicits._

  val input4 = (0 to 2).toDF("id4")
  val input5 = (0 to 2).toDF("id5")
}

object ShufflePartitionCoalesceNotAppliedAfterRepartitioning extends App {

  val input5Repartitioned = ShufflePartitionCoalesceConfiguration.input5.repartition(10)
  ShufflePartitionCoalesceConfiguration.input4.join(
    right = input5Repartitioned,
    joinExprs = ShufflePartitionCoalesceConfiguration.input4("id4") === ShufflePartitionCoalesceConfiguration.input5("id5"),
    joinType = "inner"
  ).collect()

}

object ShufflePartitionCoalesceApplied extends App {

  ShufflePartitionCoalesceConfiguration.input4.join(
    right = ShufflePartitionCoalesceConfiguration.input5,
    joinExprs = ShufflePartitionCoalesceConfiguration.input4("id4") === ShufflePartitionCoalesceConfiguration.input5("id5"),
    joinType = "inner"
  ).collect()

}