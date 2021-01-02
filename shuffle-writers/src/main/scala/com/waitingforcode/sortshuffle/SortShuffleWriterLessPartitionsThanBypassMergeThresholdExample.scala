package com.waitingforcode.sortshuffle

import org.apache.spark.{SparkConf, SparkContext}

object SortShuffleWriterLessPartitionsThanBypassMergeThresholdExample extends App {

  val conf = new SparkConf().setAppName("SortShuffleWriter").setMaster("local")
  var sparkContext = SparkContext.getOrCreate(conf)

  val numbersRdd = sparkContext.parallelize(1 to 20, 2)
  val sumComputation = (v1: Int, v2: Int) => v1 + v2

  val treeSum = numbersRdd.treeReduce(sumComputation, 2)
}

