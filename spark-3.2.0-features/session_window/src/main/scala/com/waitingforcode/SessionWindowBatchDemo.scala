package com.waitingforcode

import org.apache.spark.sql.functions.session_window
import org.apache.spark.sql.{SparkSession, functions}

import java.sql.Timestamp

object SessionWindowBatchDemo extends App {


  val sparkSession = SparkSession.builder()
    .appName("Session window demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2) // 2 physical state stores; one per shuffle partition task
    .getOrCreate()

  import sparkSession.implicits._

  val usersLogs = Seq(
    (1, "a", Timestamp.valueOf("2020-01-01 20:00:00")),
    (1, "b", Timestamp.valueOf("2020-01-01 20:02:00")),
    (1, "c", Timestamp.valueOf("2020-01-01 20:40:00")),
    (2, "aa", Timestamp.valueOf("2020-01-01 21:00:00"))
  ).toDF("user_id", "log_value", "event_time")

  val window = usersLogs
    .groupBy(session_window($"event_time", "10 minutes") as 'session, 'user_id)
    .agg(functions.count("*").as("numEvents"))

  window.show(false)

}
