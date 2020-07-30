package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.PartialMapperPartitionSpec

object AdaptiveQueryExecutionLocalShuffleReader extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Adaptive Query Execution").master("local[*]")
    .config("spark.sql.adaptive.enabled", true)
    .config("spark.sql.autoBroadcastJoinThreshold", "80B")
    .config("spark.sql.adaptive.coalescePartitions.enabled", false)
    // Enable/disable to see what happens when the local shuffle reader
    // is not added to the plan
    .config("spark.sql.adaptive.localShuffleReader.enabled", true)
    .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", 0.00000001d)
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
  import sparkSession.implicits._

  val input4 = sparkSession.sparkContext.parallelize(
    (1 to 200).map(nr => TestEntryKV(nr, nr.toString)), 2).toDF()
  input4.createOrReplaceTempView("input4")
  val input5 = sparkSession.sparkContext.parallelize(Seq(
    TestEntryKV(1, "1"), TestEntryKV(1, "2"),
    TestEntryKV(2, "1"), TestEntryKV(2, "2"),
    TestEntryKV(3, "1"), TestEntryKV(3, "2")
  ), 2).toDF()
  input5.createOrReplaceTempView("input5")

  val sqlQuery = "SELECT * FROM input4 JOIN input5 ON input4.key = input5.key WHERE input4.value = '1'"
  val selectFromQuery = sparkSession.sql(sqlQuery)
  selectFromQuery.collect()

}

object TestDivide extends App {
  def equallyDivide(numReducers: Int, numBuckets: Int): Seq[Int] = {
    val elementsPerBucket = numReducers / numBuckets
    val remaining = numReducers % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    (0 until remaining).map(_ * (elementsPerBucket + 1)) ++
      (remaining until numBuckets).map(i => splitPoint + (i - remaining) * elementsPerBucket)
  }

  val numReducers = 5
  val numMappers = 2
  val expectedParallelism = 10
  println(equallyDivide(numReducers, math.max(1, expectedParallelism / numMappers)))
  println(equallyDivide(4, 2))
  println(equallyDivide(5, 2))
  println(equallyDivide(15, 2))


  val splitPoints = equallyDivide(numReducers, math.max(1, expectedParallelism / numMappers))

  val r = (0 until numMappers).flatMap { mapIndex =>
    (splitPoints :+ numReducers).sliding(2).map {
      case Seq(start, end) => PartialMapperPartitionSpec(mapIndex, start, end)
    }
  }
  println(s"Result=${r.mkString(",")}")
}