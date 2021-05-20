package com.waitingforcode.sortshuffle

import org.apache.spark.{SparkConf, SparkContext}

object SortShuffleBlockIdExample extends App {

  val conf = new SparkConf().setAppName("SortShuffleWriter").setMaster("local")
  var sparkContext = SparkContext.getOrCreate(conf)

  val inputData = (1 to 20).map(nr => (nr % 3, nr))
  val numbersRdd = sparkContext.parallelize(inputData, 2)
  val sumComputation = (v1: Int, v2: Int) => v1 + v2

  // 4 shuffle partitions
  // You'll see that it creates 1 shuffle data file
  // per task
  val treeSum = numbersRdd.reduceByKey(sumComputation, 4)

  treeSum.collect()

}
