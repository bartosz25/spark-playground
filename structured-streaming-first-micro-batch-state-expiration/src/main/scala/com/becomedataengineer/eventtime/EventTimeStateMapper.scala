package com.becomedataengineer.eventtime

import com.becomedataengineer.{UserWithVisits, VisitTimeAndPage}
import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp

object EventTimeStateMapper {

  def countItemsForLabel(timeoutDurationMs: Long)(userId: Int, rows: Iterator[VisitTimeAndPage],
                                                  currentStateGroup: GroupState[CounterWithMaxEventTime]): Option[UserWithVisits] = {
    if (currentStateGroup.hasTimedOut) {
      println(s"State timed out!")
      val countToReturn = currentStateGroup.get
      currentStateGroup.remove()
      Some(UserWithVisits(userId, countToReturn.count))
    } else {
      val currentStateCount = currentStateGroup.getOption.getOrElse(CounterWithMaxEventTime(0, new Timestamp(0L)))
      val (countToAdd, maxTimestampFromRowData) = getStatsForState(rows)
      val newState = currentStateCount.copy(
        count = currentStateCount.count + countToAdd,
        maxEventTimeSoFar = if (maxTimestampFromRowData.after(currentStateCount.maxEventTimeSoFar)) {
          maxTimestampFromRowData
        } else {
          currentStateCount.maxEventTimeSoFar
        }
      )
      currentStateGroup.update(newState)

      val expirationTime = newState.maxEventTimeSoFar.getTime + timeoutDurationMs
      println(s"State will expire at ${new Timestamp(expirationTime)}")
      currentStateGroup.setTimeoutTimestamp(expirationTime)

      None
    }
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
