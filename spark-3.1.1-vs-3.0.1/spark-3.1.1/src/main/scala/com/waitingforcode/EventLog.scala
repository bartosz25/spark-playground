package com.waitingforcode

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp

case class EventLog(id: Int, eventTime: Timestamp)

object EventLog {
  val Schema = ScalaReflection.schemaFor[EventLog].dataType.asInstanceOf[StructType]
}