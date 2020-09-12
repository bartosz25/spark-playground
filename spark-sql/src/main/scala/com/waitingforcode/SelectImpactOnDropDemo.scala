package com.waitingforcode

import org.apache.spark.sql.SparkSession

object SelectImpactOnDropDemo extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("Select vs Drop").master("local[*]")
    .getOrCreate()
  import testSparkSession.implicits._

  val dataset = Seq(
    (0, "a", "A"), (1, "b", "B"), (2, "c", "C")
  ).toDF("nr", "lower_letter", "upper_letter")

  println("----- Select-based reduction ------")
  val selectBasedReduction = dataset.select("lower_letter", "upper_letter")
  selectBasedReduction.show()
  selectBasedReduction.explain(true)

  println("----- Drop-based reduction ------")
  val dropBasedReduction = dataset.drop("nr")
  dropBasedReduction.show()
  dropBasedReduction.explain(true)

}
