package com.waitingforcode.sortshuffle

import org.apache.spark.{SparkConf, SparkContext}

object SortShuffleWriterSpillingExample extends App {

  val conf = new SparkConf().setAppName("SortShuffleWriter").setMaster("local")
  conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", "500")
  conf.set("spark.shuffle.sort.bypassMergeThreshold", "1")
  var sparkContext = SparkContext.getOrCreate(conf)

  val numbersRdd = sparkContext.parallelize(1 to 2000, 2)
  numbersRdd.map(nr => (nr % 2, nr)).reduceByKey((nr1, nr2) => nr1 + nr2)
    .collectAsMap()

}
