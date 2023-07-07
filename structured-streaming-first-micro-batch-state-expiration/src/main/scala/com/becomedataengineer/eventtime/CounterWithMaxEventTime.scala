package com.becomedataengineer.eventtime

import java.sql.Timestamp

case class CounterWithMaxEventTime(count: Int, maxEventTimeSoFar: Timestamp)
