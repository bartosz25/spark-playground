package com.waitingforcode

import org.apache.spark.{SparkConf, SparkContext}

object CoalesceDemo extends App {

  val conf = new SparkConf().setAppName("Test coalesce").setMaster("local[*]")
  val sparkContext = SparkContext.getOrCreate(conf)

  new StaticLocationsRdd(sparkContext, Seq.empty).coalesce(5, shuffle = false)
    .collect()

}
