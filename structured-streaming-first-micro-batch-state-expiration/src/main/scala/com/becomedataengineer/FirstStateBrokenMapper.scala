package com.becomedataengineer

import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp

object FirstStateBrokenMapper {


  def countItemsForLabel(timeoutDurationMs: Long)(userId: Int, rows: Iterator[VisitTimeAndPage],
                                                  currentState: GroupState[Int]): Option[UserWithVisits] = {
    if (currentState.hasTimedOut) {
      println(s"State timed out!")
      val countToReturn = currentState.get
      currentState.remove()
      Some(UserWithVisits(userId, countToReturn))
    } else {
      val currentStateCount = currentState.getOption.getOrElse(0)
      val newCount = currentStateCount + rows.size
      currentState.update(newCount)

      val expirationTime = currentState.getCurrentWatermarkMs() + timeoutDurationMs
      println(s"State will expire at ${new Timestamp(expirationTime)}")
      currentState.setTimeoutTimestamp(expirationTime)

      None
    }
  }
}