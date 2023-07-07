package com.becomedataengineer.temporarystate

import com.becomedataengineer.{UserWithVisits, VisitTimeAndPage}
import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp

object FlagStateMapper {

  def countItemsForLabel(timeoutDurationMs: Long)(userId: Int, rows: Iterator[VisitTimeAndPage],
                                                  currentStateGroup: GroupState[CounterWithFlag]): Option[UserWithVisits] = {
    val expirationTime = currentStateGroup.getCurrentWatermarkMs() + timeoutDurationMs
    if (currentStateGroup.hasTimedOut) {
      println(s"State timed out!")
      val currentState = currentStateGroup.get
      if (didStateReallyExpire(currentState, currentStateGroup.getCurrentWatermarkMs())) {
        currentStateGroup.remove()
        Some(UserWithVisits(userId, currentState.count))
      } else {
        currentStateGroup.update(currentState)

        println(s"Setting new expiration time as at ${new Timestamp(expirationTime)}")
        currentStateGroup.setTimeoutTimestamp(expirationTime)
        None
      }
    } else {
      val (countToAdd, maxEventTime) = getStatsForState(rows)
      val (needsUpdateWithNewWatermark, eventTimeForState) =
        if (currentStateGroup.getCurrentWatermarkMs() > 0) {
          (false, None)
        } else {
          (true, Some(maxEventTime))
        }
      val currentState = currentStateGroup.getOption.getOrElse(CounterWithFlag(count = 0))
      currentStateGroup.update(currentState.copy(
        count = currentState.count + countToAdd,
        needsUpdateWithNewWatermark = needsUpdateWithNewWatermark,
        firstGroupMaxEventTime = eventTimeForState
      ))

      println(s"State will expire at ${new Timestamp(expirationTime)}")
      currentStateGroup.setTimeoutTimestamp(expirationTime)

      None
    }
  }

  private def didStateReallyExpire(currentState: CounterWithFlag,
                                   currentWatermarkMs: Long): Boolean = {
    (currentState.needsUpdateWithNewWatermark &&
      currentState.firstGroupMaxEventTime.get.getTime < currentWatermarkMs) ||
      !currentState.needsUpdateWithNewWatermark
  }

  private def getStatsForState(rows: Iterator[VisitTimeAndPage]): (Int, Timestamp) = {
    var maxTimestamp: Timestamp = null
    var rowsNumber = 0
    while (rows.hasNext) {
      val nextRow = rows.next()
      rowsNumber += 1
      if (maxTimestamp == null || nextRow.eventTime.after(maxTimestamp)) {
        maxTimestamp = nextRow.eventTime
      }
    }
    (rowsNumber, maxTimestamp)
  }

}
