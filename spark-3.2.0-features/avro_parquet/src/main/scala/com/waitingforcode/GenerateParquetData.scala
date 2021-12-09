package com.waitingforcode

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp

object GenerateParquetData {

  def main(args: Array[String]) = {
    // Run this code with Apache Spark 2.4.5 profile
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34377 : Parquet date time mode at a source level").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    Seq((1, Timestamp.valueOf("1000-01-01 20:23:33.3333"))).toDF("id", "event_time")
      .write.format("parquet").mode(SaveMode.Overwrite).save("/tmp/spark245-input")

  }
}
