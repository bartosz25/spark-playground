package com.waitingforcode

import org.apache.spark.sql.SparkSession

object SkewedJoinOptimizationConfiguration {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Adaptive Query Execution - join skew optimization")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", true)
    // First, disable all configs that would create a broadcast join
    .config("spark.sql.autoBroadcastJoinThreshold", "1")
    .config("spark.sql.join.preferSortMergeJoin", "true")
    .config("spark.sql.adaptive.logLevel", "TRACE")
    // Disable coalesce to avoid the coalesce condition block the join skew
    // optimization happen
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    // Finally, configure the skew demo
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "10KB")
    // Required, otherwise the skewed partitions are too small to be optimized
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1B")
    .getOrCreate()
  import sparkSession.implicits._


  val input4 = (0 to 50000).map(nr => {
    if (nr < 30000) {
      1
    } else {
      nr
    }
  }).toDF("id4")
  input4.createOrReplaceTempView("input4")
  val input5 =  (0 to 300).map(nr => nr).toDF("id5")
  input5.createOrReplaceTempView("input5")
  val input6 = (0 to 300).map(nr => nr).toDF("id6")
  input6.createOrReplaceTempView("input6")

}

object SkewedJoinOptimizationNotAppliedDemo extends App {

  SkewedJoinOptimizationConfiguration.sparkSession.sql(
    """
      |SELECT * FROM input4 JOIN input5 ON input4.id4 = input5.id5
      |JOIN input6 ON input6.id6 = input5.id5""".stripMargin
  ).collect()

}

object SkewedJoinOptimizationAppliedDemo extends App  {

  SkewedJoinOptimizationConfiguration.sparkSession
    .sql("SELECT * FROM input4 JOIN input5 ON input4.id4 = input5.id5")
    .collect()
}