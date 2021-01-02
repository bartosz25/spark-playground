package com.waitingforcode.bypassmerge

import org.apache.spark.{SparkConf, SparkContext}

object BypassMergeAndRddWithoutMapSideCombineExample extends App {

  val conf = new SparkConf().setAppName("BypassMerge shuffle writer").setMaster("local")
  var sparkContext = SparkContext.getOrCreate(conf)

  val numbersRdd = sparkContext.parallelize(1 to 20, 2)

  val oddAndEvenNumbers = numbersRdd.map(nr => (nr % 2, nr))
    .groupByKey().mapValues(numbers => numbers.count(_ => true))
    .collect()


}
