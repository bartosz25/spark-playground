package com.waitingforcode

import org.apache.spark.sql.SparkSession

object IterativeExecutionWithCheckpoint {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    sparkSession.sparkContext.setCheckpointDir("/tmp/spark-sql/checkpoint")
    import sparkSession.implicits._

    val rawInput = (0 to 5).map(nr => (nr, s"label=${nr}", nr % 3 == 0)).toDF("nr", "label", "modulo")
    var iterativeDataset = rawInput.filter("nr > 1").selectExpr("nr * 2 AS nr")
    (0 to 3).foreach(nr => {
      if (nr % 2 == 0) {
        iterativeDataset = iterativeDataset.checkpoint(true)
      }
      iterativeDataset = iterativeDataset.filter(s"nr > ${nr}").selectExpr(s"(nr * ${nr}) AS nr")
    })
    iterativeDataset.explain(true)
  }

}
