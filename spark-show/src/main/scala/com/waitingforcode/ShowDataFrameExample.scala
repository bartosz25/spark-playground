package com.waitingforcode
import org.apache.spark.sql.SparkSession

object ShowDataFrameExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val dfData = (0 to 100).map(n => (n, n*2, n*3)).toDF("nr", "nr_2", "nr_3").repartition(10)

    dfData.show(numRows=5, truncate=false)
    dfData.show(numRows=5, truncate=0, vertical=true)

    while (true) {}

  }

}