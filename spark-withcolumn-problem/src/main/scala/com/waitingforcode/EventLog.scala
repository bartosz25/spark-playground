package com.waitingforcode

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

case class EventLog(visit_id: String, user_id: Long, event_time: Timestamp, page: Page, source: Source,
                    user: Option[User], technical: Technical, keep_private: Boolean)
case class Page(current: String, previous: Option[String])
case class Source(site: String, api_version: String)
case class User(ip: String, latitude: Double, longitude: Double)
case class Technical(browser: String, os: String, lang: String, network: String, device: Option[Device])
case class NetworkStruct(short_name: String, long_name: String)
case class BrowserStruct(name: String, language: String)
case class Device(`type`: String, version: Option[String])
case class DeviceTypeStruct(name: String)


object EventLog {
  val Schema = ScalaReflection.schemaFor[EventLog].dataType.asInstanceOf[StructType]
}