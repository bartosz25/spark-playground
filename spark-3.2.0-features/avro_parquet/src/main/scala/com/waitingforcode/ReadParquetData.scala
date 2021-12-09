package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ReadParquetData {

  def main(args: Array[String]) = {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34377 : Parquet date time mode at a source level").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    sparkSession.read.format("parquet").option("datetimeRebaseMode", "EXCEPTION")
      .load("/tmp/spark245-input").show(false)
  }
}
