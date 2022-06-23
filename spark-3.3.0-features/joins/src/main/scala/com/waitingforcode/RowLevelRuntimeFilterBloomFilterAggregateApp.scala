package com.waitingforcode

import org.apache.spark.sql.{SaveMode, SparkSession}

object RowLevelRuntimeFilterBloomFilterAggregateApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. joins").master("local[*]")
      .config("spark.sql.optimizer.runtime.bloomFilter.enabled", true)
      .config("spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled", false)
      .config("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "4000B")
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.sql.autoBroadcastJoinThreshold", 3000) // 2368B for the size check in the subquery condition
      .getOrCreate()

    import sparkSession.implicits._
    val dataset1 = "/tmp/dataset1"
    val numbersWithLetters = (1 to 100).map(nr => (nr, s"${nr.toString}")).toDF("number1", "letter1")
            numbersWithLetters.write.format("parquet").mode(SaveMode.Overwrite).save(dataset1)
    val dataset2 = "/tmp/dataset2"
    val lettersWithNumbers = (1 to 10).map(nr => (nr, s"${nr.toString}")).toDF("number2", "letter2")
    lettersWithNumbers.write.format("parquet").mode(SaveMode.Overwrite).save(dataset2)

    sparkSession.read.parquet(dataset1).createOrReplaceTempView("dataset1")
    sparkSession.read.parquet(dataset2).createOrReplaceTempView("dataset2")
    sparkSession.sql(
      """
        |SELECT * FROM dataset1
        |LEFT JOIN dataset2 ON dataset1.number1 = dataset2.number2
        |WHERE dataset1.letter1 = 'A'
        |""".stripMargin).explain(true)

  }

}
