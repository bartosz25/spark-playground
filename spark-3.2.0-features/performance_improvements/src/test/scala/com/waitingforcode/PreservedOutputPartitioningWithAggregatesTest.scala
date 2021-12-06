package com.waitingforcode

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class PreservedOutputPartitioningWithAggregatesTest extends AnyFlatSpec  {

  // It's a test without assertions but with print, so that it can be run
  // with Maven profiles to check the executed plans
  it should "preserve output partitioning for aggregates" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34593 : preserve output partitioning").master("local[*]")
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.range(15).toDF("number").write.mode(SaveMode.Overwrite)
      .bucketBy(4, "number").saveAsTable("t1")
    sparkSession.range(6).toDF("number")
      .write.mode(SaveMode.Overwrite).bucketBy(4, "number").saveAsTable("t2")
    sparkSession.sql(
      """
        |SELECT number, COUNT(*) FROM (
        | SELECT /*+ BROADCAST(t2) */ t1.number FROM t1 LEFT OUTER JOIN  t2
        |) GROUP BY number
        |""".stripMargin).explain(false)
  }

}
