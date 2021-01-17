package com.waitingforcode

import org.apache.spark.sql.SparkSession

object RepartitionDemo extends App {

  val  sparkSession = SparkSession.builder()
    .appName("Repartition").master("local[*]")
    .config("spark.default.parallelism", 3)
    // TODO: disable sortbefore!
    //.config("spark.sql.execution.sortBeforeRepartition", false)
    .getOrCreate()
  import sparkSession.implicits._

  val inputNumbers = (0 to 11).toDF("nr")

  inputNumbers.repartition(2)
    .mapPartitions(rows => {
      println(s"Rows in partition: ${rows.mkString(",")}")
      Iterator("a")
    }).show()

}
