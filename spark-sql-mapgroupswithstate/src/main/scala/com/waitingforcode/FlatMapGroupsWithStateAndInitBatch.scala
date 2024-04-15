package com.waitingforcode

import org.apache.spark.sql.{KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.OutputMode.Update

import java.sql.Timestamp

object FlatMapGroupsWithStateAndInitBatch {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    import sparkSession.implicits._

    val timestampedEvents = Seq(
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:00:00")),
      TimestampedEvent(2, Timestamp.valueOf("2024-04-01 09:02:00")),
      TimestampedEvent(3, Timestamp.valueOf("2024-04-01 09:04:00")),
      TimestampedEvent(4, Timestamp.valueOf("2024-04-01 09:12:50")),
      TimestampedEvent(1, Timestamp.valueOf("2024-04-01 09:00:00")),
      TimestampedEvent(2, Timestamp.valueOf("2024-04-01 10:02:50"))
    ).toDS

    val initialStateDf: KeyValueGroupedDataset[Int, Seq[String]] = Seq(
      (1, Seq("init for 1: 1=1")),
      (10, Seq("init for 10: 10=10")),
    ).toDF("id", "stateValue").as[(Int, Seq[String])].groupByKey(row => row._1).mapValues(row => row._2)

    val query = timestampedEvents.withWatermark("eventTime", "20 minutes")
      .groupByKey(row => row.eventId)
      .flatMapGroupsWithState(
        outputMode = Update(),
        timeoutConf = GroupStateTimeout.EventTimeTimeout(),
        initialState = initialStateDf)(
        func = StatefulMappingFunction.concatenateRowsInGroup)

    query.explain(true)
    query.show(truncate=false)
  }
}
