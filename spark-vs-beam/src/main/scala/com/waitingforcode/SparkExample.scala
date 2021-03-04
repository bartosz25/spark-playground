package com.waitingforcode

import org.apache.spark.sql.SparkSession

object SparkExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Apache Spark vs Apache Beam API").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()
  import sparkSession.implicits._

  val inputNumbers = (0 to 11).toDF("nr")

  val groupedNumbersGreaterThan3 = inputNumbers.filter("nr > 3").as[Int]
    .groupByKey(nr => nr % 2 == 0)

  val labelsWithSums = groupedNumbersGreaterThan3.mapGroups((evenOddFlag, numbers) => {
      val label = if (evenOddFlag) "even"  else "odd"
      (label, numbers.sum)
    })
    .withColumnRenamed("_1", "label")
    .withColumnRenamed("_2", "sum")

  labelsWithSums.write.mode("overwrite").json("/tmp/spark-vs-beam/spark")
}
