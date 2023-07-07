package com.becomedataengineer

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp

case class VisitTimeAndPage(userId: Int, page: String, eventTime: Timestamp)


object VisitTimeAndPage {
  val Schema = ScalaReflection.schemaFor[VisitTimeAndPage].dataType.asInstanceOf[StructType]
}