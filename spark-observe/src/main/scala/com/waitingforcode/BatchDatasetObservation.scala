package com.waitingforcode
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Observation, SparkSession, functions}

import scala.collection.JavaConverters._

object BatchDatasetObservation {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark observe").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val input = (0 to 99).toDF("nr")

    val observation = Observation("batch_test")
    val observedDataset = input.observe(observation, functions.count(lit(1))
      .as("rows"), functions.max($"nr").as("max_nr"))


    sparkSession.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.observedMetrics.get("batch_test").foreach { batchTestRow => {
          println("count = " + batchTestRow.getAs[Int]("rows"))
          println("max = " + batchTestRow.getAs[Int]("max_nr"))
        }}
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    })
    observedDataset.count()
  }

}


