package com.waitingforcode

import org.apache.spark.sql.SparkSession

trait AdaptiveQueryExecutionDoReuseAdaptiveSubqueryConfiguration {

  def runTest(enabledReuse: Boolean): Unit = {
    val sparkSession =
      SparkSession.builder()
        .appName("Spark 3.0: Adaptive Query Execution").master("local[*]")
        .config("spark.sql.adaptive.enabled", true)
        .config("spark.sql.join.preferSortMergeJoin", true)
        .config("spark.sql.autoBroadcastJoinThreshold", "1B")
        .config("spark.sql.adaptive.logLevel", "TRACE")
        .config("spark.sql.execution.reuseSubquery", enabledReuse)
        // Disable other AQE rules
        .config("spark.sql.adaptive.coalescePartitions.enabled", false)
        .config("spark.sql.adaptive.skewJoin.enabled", false)
        .getOrCreate()
    import sparkSession.implicits._

    val input4 = (0 to 3000).toDF("id4")
    input4.createOrReplaceTempView("input4")
    val input5 = (0 to 200).toDF("id5")
    input5.createOrReplaceTempView("input5")
    val input6 = (0 to 200).toDF("id6")
    input6.createOrReplaceTempView("input6")


    val subqueryRepeatedMax =
      """
        |SELECT (SELECT MAX(id5) FROM input5) AS max1, (SELECT MAX(id5) FROM input5) AS max2 FROM
        |input4 JOIN input6 ON input6.id6 = input4.id4
        |""".stripMargin

    sparkSession.sql(subqueryRepeatedMax).collect()
  }

}


object AdaptiveQueryExecutionDoNotReuseAdaptiveSubqueryDemo extends App
  with AdaptiveQueryExecutionDoReuseAdaptiveSubqueryConfiguration {

  runTest(false)

}

object AdaptiveQueryExecutionDoReuseAdaptiveSubqueryDemo extends App with
  AdaptiveQueryExecutionDoReuseAdaptiveSubqueryConfiguration {

  runTest(true)


}
