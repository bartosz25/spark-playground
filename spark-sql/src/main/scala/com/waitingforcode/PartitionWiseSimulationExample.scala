package com.waitingforcode

import org.apache.spark.sql.SparkSession

object PartitionWiseSimulationExample extends App {

  val dataset1Location = "/tmp/spark/3/partitionwise_simulation/dataset1"
  val dataset2Location = "/tmp/spark/3/partitionwise_simulation/dataset2"
  val  sparkSession = SparkSession.builder()
    .appName("Partition-wise joins simulation").master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", 7200) // one side has 7217 bytes
    .config("spark.sql.adaptive.enabled", true)
    .getOrCreate()
  import sparkSession.implicits._

  val tableName = s"orders"
  val tableName2 = s"orders_2"
  val orders = (0 to 200).map(orderId => (orderId, s"user${orderId}", orderId%3))
    .toDF("order_id", "user_id", "partition_number")

  orders.write.mode("overwrite").partitionBy("partition_number")
    .json(dataset1Location)
  orders.write.mode("overwrite").partitionBy("partition_number")
    .json(dataset2Location)

  def listAllPartitions = Seq(0, 1, 2) // static for tests, but can use data catalog to get this info

  val dataset1Cached = sparkSession.read.json(dataset1Location).cache()
  val dataset2Cached = sparkSession.read.json(dataset2Location).cache()
  val partitionWiseJoins = listAllPartitions.map(partitionNumber => {
    // It's important to "push down" the partition predicate to make
    // the dataset smaller and eventually take advantage of small dataset
    // optimizations like broadcast joins
    val dataset1ForPartition = sparkSession.read.json(s"${dataset1Location}/partition_number=${partitionNumber}")
    val dataset2ForPartition = sparkSession.read.json(s"${dataset2Location}/partition_number=${partitionNumber}")
    dataset1ForPartition.join(dataset2ForPartition,
      dataset1ForPartition("order_id") === dataset2ForPartition("order_id"))
  })

  val unions = partitionWiseJoins.reduce((leftDataset, rightDataset) => leftDataset.unionAll(rightDataset))
  unions.explain(true)

  sparkSession.read.json(dataset1Location).join(
    sparkSession.read.json(dataset2Location), "order_id"
  ).explain(true)
}