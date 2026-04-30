package com

import org.apache.spark.sql.SparkSession

import java.util.TimeZone

package object waitingforcode {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  val sparkSession = SparkSession.builder().master(s"local[*]")
    .config("spark.sql.shuffle.partitions", 1).config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()



}
