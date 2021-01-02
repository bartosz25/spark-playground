package com.waitingforcode.unsafewriter

import org.apache.spark.{SparkConf, SparkContext}

// used writer: SortShuffleWriter
// unsafe writer doesn't support serialized objects
object MapSideCombineWithSerializedShuffleExample extends App {

  val conf = new SparkConf().setAppName("Tree aggregation for SortShuffleWriter").setMaster("local")
  var sparkContext = SparkContext.getOrCreate(conf)

  val numbersRdd = sparkContext.parallelize(1 to 20, 10)
  val sumComputation = (v1: Int, v2: Int) => v1 + v2

  val treeSum = numbersRdd.treeReduce(sumComputation, 2)


}
