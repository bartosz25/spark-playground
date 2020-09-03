package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream

object StreamingAggregationLimit245 extends App {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming aggregation with limit")
    .config("spark.sql.shuffle.partitions", 2)
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  val testKey = "other-processing-append-output-mode"
  val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
  val mappedResult = inputStream.toDS().toDF("nr", "letter")
  inputStream.addData(
    (1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"), (6, "F")
  )

  val query = mappedResult.limit(2).groupBy("nr").count().writeStream.outputMode("complete")
    .option("truncate", false)
    .format("console")
    .start()
  new Thread(new Runnable() {
    override def run(): Unit = {
      while (!query.isActive) {}
      Thread.sleep(5000)
      println("Adding new results")
      inputStream.addData(
        (6, "F"), (5, "E"), (4, "D"),  (3, "C"), (2, "B"),  (1, "A")
      )
      query.explain(true)
    }
  }).start()
  query.awaitTermination(30000)



}

