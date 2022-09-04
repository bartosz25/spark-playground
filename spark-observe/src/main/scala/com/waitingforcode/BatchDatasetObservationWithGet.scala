package com.waitingforcode

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Observation, SparkSession, functions}

object BatchDatasetObservationWithGet {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark observe").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val input = (0 to 99).toDF("nr")

    val observation = Observation("batch_test")
    val observedDataset = input.observe(observation, functions.count(lit(1))
      .as("rows"), functions.max($"nr").as("max_nr"))

    observedDataset.count()

    println("all rows="+observation.get("rows"))
    println("max number="+observation.get("max_nr"))
  }

}


