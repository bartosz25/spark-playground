package com.waitingforcode

import org.apache.spark.sql.streaming.GroupState

object StatefulMappingFunction {

  def concatenateRowsInGroup(key: Int, values: Iterator[TimestampedEvent],
                             state: GroupState[Seq[String]]): Iterator[String] = {
    println(s"State is=${state}")
    //println(s"State is=${state.getCurrentWatermarkMs()}")
    if (values.isEmpty && state.hasTimedOut) {
      Seq(s"${state.get}: timed-out").iterator
    } else {
      if (state.getOption.isEmpty) state.setTimeoutTimestamp(3000)
      val stateNames = state.getOption.getOrElse(Seq.empty)
      val stateNewNames = stateNames ++ values.map(row => row.eventTime.toString)
      state.update(stateNewNames)
      stateNewNames.iterator
    }
  }
}
