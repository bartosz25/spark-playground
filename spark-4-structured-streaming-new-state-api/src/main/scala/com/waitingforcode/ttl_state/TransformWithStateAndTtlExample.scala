package com.waitingforcode.ttl_state

import com.waitingforcode.{Session, SessionPage, Visit}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Encoders, SparkSession}

import java.sql.Timestamp
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import scala.math.max

object TransformWithStateAndTtlExample  {

  def main(args: Array[String]): Unit = {
    import com.waitingforcode._
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.shuffle.partitions", 1).config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    import sparkSession.implicits._

    val initialState = Seq(
      InitialSessionState(
        visitId = "visit1", user_id = None, device = InitialSessionStateDevice(device_version = "10", device_type = "mac"),
        visits = Seq(SessionPage(page = "home", visit_time = "10:00:00"),  SessionPage(page = "contact", visit_time = "10:01:00"))
      ),
      InitialSessionState(
        visitId = "visit1", user_id = None, device = InitialSessionStateDevice(device_version = "10", device_type = "mac"),
        visits = Seq(SessionPage(page = "home_2", visit_time = "11:00:00"),  SessionPage(page = "contact_2", visit_time = "11:01:00"))
      )
    ).toDS().groupByKey(r => r.visitId)

    val visitsInputStream = new MemoryStream[Visit](1, sparkSession.sqlContext, Some(2))
    val visitsInput = visitsInputStream.toDS()
    val groupedVisits = visitsInput.withWatermark("event_time", "5 minutes")
      .groupByKey(row => row.visit_id)

    val sessionsMaker = groupedVisits.transformWithState(
      statefulProcessor=new TransformWithStateHolder.SessionProcessor(),
      timeMode=TimeMode.EventTime,
      outputMode=OutputMode.Update(),
      initialState = initialState
    )

    val query = sessionsMaker.writeStream.format("console").option("truncate", false).start()

    visitsInputStream.addData(Seq(
      Visit.visit("visit1", "10:00:00", "alerts"),
      Visit.visit("visit1", "10:01:00", "contact"),
      Visit.visit("visit1", "10:02:00", "cart"),
      Visit.visit("visit2", "10:01:00", "start_session", user_id = Some("user_1")),
      Visit.visit("visit2", "10:02:00", "home", user_id = Some("user_1"))
    ))
    query.processAllAvailable()
    // watermark = 09:57 (10:02 - 5m)
    // expiration times: visit1 (10:02 + 40m = 10:42), visit2 (10:02 + 40m = 10:42)

    visitsInputStream.addData(Seq(
      Visit.visit("visit3", "10:30:00", "alerts")
    ))
    query.processAllAvailable()
    // watermark = 10:25 (10:30 - 5m)
    // expiration times: visit3 (09:57 + 40m = 10:37)

    visitsInputStream.addData(Seq(
      Visit.visit("visit3", "10:47:00", "alerts")
    ))
    query.processAllAvailable()
    // watermark = 10:42 (10:47 - 5m)
    // expiration times: visit3 (10:25 + 40m = 11:05)

  }

}

object TransformWithStateHolder {

  class SessionProcessor extends StatefulProcessorWithInitialState[String, Visit, Session, InitialSessionState] {

    private val Time10MinutesAsMillis = TimeUnit.MINUTES.toMillis(40)

    @transient private var currentSession: ValueState[SessionState] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      // Init comes first, even before handleInitialState
      // OutputMode is NoOp here, i;e. the logic must comply with the output mode but the design doc explains that
      // "We already provide the ability for users to define the output mode for a streaming query.
      // We extend that functionality to also allow for the transformWithState operator to define its own output mode
      // depending on the type of output it will produce. The reason to specify an additional argument is to eventually
      // allow for chained operators to potentially specify different output modes (currently with multiple stateful operators
      // - we only support the Append output mode). Similar to streaming aggregation queries, it’s possible that users might
      // perform operations based on all supported output modes (append, update and complete) within the transformWithState
      // operator. For supporting complete mode, it’s possible that all the keys might not be part of a specific
      // trigger - so users might have to iterate on one or more state instances to get all the required keys and generate the
      // required output. Note that we cannot guarantee that the StateProcessor output actually obeys the requirements of the
      // output mode. Ensuring those semantics are implemented correctly is the responsibility of the user."
      // https://docs.google.com/document/d/1QjZmNZ-fHBeeCYKninySDIoOEWfX6EmqXs2lK097u9o/edit?tab=t.0#heading=h.rkdh75zdoie6
      println(s"Initializing state with ${outputMode} and ${timeMode}")
      // set the TTL to NONE as we don't want to have this state to expire without the watermark
      currentSession =  getHandle.getValueState("session", Encoders.product[SessionState], TTLConfig.NONE)
    }

    override def handleInitialState(key: String, initialState: InitialSessionState, timerValues: TimerValues): Unit = {
      print(s"Handling initial state ${initialState} for ${key} and ${timerValues}")
      val sessionState = initialState.toSessionState
      val lastVisitedPage = sessionState.visits.maxBy(page => page.visit_time)
      currentSession.update(initialState.toSessionState)
      val newExpirationTime = lastVisitedPage.visit_time.getTime + Time10MinutesAsMillis
      println(s"Setting expiration time as ${lastVisitedPage.visit_time} + 40 minutes = ${new Timestamp(newExpirationTime + Time10MinutesAsMillis)}")
      getHandle.registerTimer(newExpirationTime)
    }


    override def handleInputRows(visitId: String, visits: Iterator[Visit], timerValues: TimerValues): Iterator[Session] = {
      println(s"Handling new rows for timer values ${timerValues}")
      val currentPages = if (currentSession.exists()) {
        currentSession.get().visits
      } else {
        Seq.empty
      }

      var baseEventTimeForTimeout: Long = timerValues.getCurrentWatermarkInMs()
      var deviceType: Option[String] = None
      var deviceVersion: Option[String] = None
      var userId: Option[String] = None
      val mappedVisits: Iterator[SessionPage] = visits.map(visit => {
        baseEventTimeForTimeout = if (timerValues.getCurrentWatermarkInMs() == 0) {
          // The watermark is missing in the first micro-batch; we're using the max event time for
          // each visit here
          max(baseEventTimeForTimeout, visit.event_time.getTime)
        } else {
          baseEventTimeForTimeout
        }
        userId = visit.user_id
        if (deviceType.isEmpty && visit.getDeviceTypeIfDefined.isDefined) {
          deviceType = visit.getDeviceTypeIfDefined
        }
        if (deviceVersion.isEmpty && visit.getDeviceVersionIfDefined.isDefined) {
          deviceVersion = visit.getDeviceVersionIfDefined
        }
        SessionPage(visit.page, visit.event_time)
      })

      println(s">> [${visitId} New Max IS ${new Timestamp(baseEventTimeForTimeout)} ")
      println(s"[${visitId} Watermark is ${new Timestamp(timerValues.getCurrentWatermarkInMs())}")
      val newState = SessionState(visits = currentPages ++ mappedVisits,
        device_type = deviceType, device_version = deviceVersion,
        user_id = userId
      )
      currentSession.update(newState)

      getHandle.listTimers().foreach(timeoutValue => {
        println(s">>>>>>> Deleting timer for ${timeoutValue} (${new Timestamp(timeoutValue)}")
        getHandle.deleteTimer(timeoutValue)
      })
      println(s"Setting expiration time as ${new Timestamp(baseEventTimeForTimeout )} + 40 minutes = ${new Timestamp(baseEventTimeForTimeout + Time10MinutesAsMillis)}")
      val newExpirationTime = baseEventTimeForTimeout + Time10MinutesAsMillis
      getHandle.registerTimer(newExpirationTime)
      val sessionState = currentSession.get()
      Iterator(sessionState.toSession(visitId = visitId, isPartial = true))
    }

    override def handleExpiredTimer(visitId: String, timerValues: TimerValues,
                                    expiredTimerInfo: ExpiredTimerInfo): Iterator[Session] = {
      println(s"State is expiring for ${visitId} and ${new Timestamp(timerValues.getCurrentWatermarkInMs())} for" +
        s" ${new Timestamp(expiredTimerInfo.getExpiryTimeInMs())} ")
      println(s"Expired timer info: ${expiredTimerInfo}")
      val sessionState = currentSession.get()
      currentSession.clear()
      Iterator(sessionState.toSession(visitId = visitId, isPartial = false))
    }
  }

}
case class InitialSessionState(visitId: String, visits: Seq[SessionPage], device: InitialSessionStateDevice, user_id: Option[String]) {
  def toSessionState: SessionState = {
    SessionState(visits = visits, device_type = Option(device.device_type),
      device_version = Option(device.device_version), user_id = user_id)
  }
}
case class InitialSessionStateDevice(device_type: String, device_version: String)
case class SessionState(visits: Seq[SessionPage], device_type: Option[String],
                        device_version: Option[String], user_id: Option[String]) {

  def toSession(visitId: String, isPartial: Boolean): Session = {
    val sortedPages = visits.sortBy(page => page.visit_time)
    val sessionStartTime = sortedPages.head.visit_time
    val sessionEndTime = sortedPages.last.visit_time
    Session(
      visit_id = visitId, user_id = user_id, start_time = sessionStartTime,
      end_time = sessionEndTime, visited_pages = sortedPages,
      device_type = device_type, device_version = device_version,
      device_full_name = None, isPartial = isPartial
    )
  }

}