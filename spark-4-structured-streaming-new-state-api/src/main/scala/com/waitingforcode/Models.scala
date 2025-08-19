package com.waitingforcode

import java.sql.Timestamp
import java.time.Instant

case class TechnicalContext(browser: String, browser_version: String, network_type: String,
                            device_type: String, device_version: String)
case class UserContext(ip: String, login: Option[String], connected_since: Option[Timestamp])
case class VisitContext(referral: String, ad_id: Option[String], user: Option[UserContext],
                        technical: Option[TechnicalContext])
case class Visit(visit_id: String, event_time: Timestamp,
                                user_id: Option[String], page: String, context: Option[VisitContext] = None) {

  def getDeviceTypeIfDefined: Option[String] = {
    context.flatMap(c => c.technical.map(t => t.device_type))
  }

  def getDeviceVersionIfDefined: Option[String] = {
    context.flatMap(c => c.technical.map(t => t.device_version))
  }
}
object Visit {

  def visit(visit_id: String, event_time: String, page: String, user_id: Option[String] = Some("user A id"),
            referral: String = "search", ad_id: String = "ad 1",
            user_context: UserContext = UserContextUnderTest.build(),
            technical_context: TechnicalContext = TechnicalContextUnderTest.build()): Visit = {
    Visit(
      visit_id = visit_id, event_time = event_time,
      user_id = user_id, page = page,
      context = Some(VisitContext(
        referral = referral, ad_id = Option(ad_id), user = Option(user_context),
        technical = Option(technical_context)
      ))
    )
  }
  object UserContextUnderTest {

    def build(ip: String = "1.1.1.1", login: String = "user A login",
              connected_since: String = "2024-01-05T11:00:00.000Z"): UserContext = {
      val connectedSinceFinalValue: Option[Timestamp] = nullableStringToOptionalTimestamp(connected_since)
      UserContext(ip = ip, login = Option(login),
        connected_since = connectedSinceFinalValue)
    }
  }

  object TechnicalContextUnderTest {


    def build(browser: String = "Firefox", browser_version: String = "3.2.1",
              network_type: String = "wifi", device_type: String ="pc",
              device_version: String = "1.2.3"): TechnicalContext = {

      TechnicalContext(browser = browser, browser_version = browser_version,
        network_type = network_type, device_type = device_type, device_version = device_version)
    }
  }
  def nullableStringToOptionalTimestamp(nullableStringTimestamp: String): Option[Timestamp] = {
    Option(nullableStringTimestamp)
      .map(timestampString => Timestamp.from(Instant.parse(timestampString)))

  }

}


case class SessionPage(page: String, visit_time: Timestamp)
case class Session(visit_id: String, user_id: Option[String], start_time: Timestamp,
                   end_time: Timestamp, visited_pages: Seq[SessionPage],
                   device_type: Option[String], device_version: Option[String],
                   device_full_name: Option[String], isPartial: Boolean)