package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.util.TimeZone

package object waitingforcode {

  def createSparkSession(): SparkSession = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master("local[2]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    sparkSession
  }

  case class Event(id: Int, eventTime: Timestamp, currentTime: String)
  val EventSchema = ScalaReflection.schemaFor[Event].dataType.asInstanceOf[StructType]
}


