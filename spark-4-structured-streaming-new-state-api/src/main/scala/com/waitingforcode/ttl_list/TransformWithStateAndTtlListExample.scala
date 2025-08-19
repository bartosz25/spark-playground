package com.waitingforcode.ttl_list

import com.waitingforcode.Visit
import org.apache.spark.sql.execution.streaming.{ListStateImplWithTTL, MemoryStream}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Encoders, SparkSession}

import java.sql.Timestamp
import java.time.Duration
import java.util.TimeZone

object ListenerForRemovedEntries extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    if (event.progress.stateOperators.nonEmpty) {
      val entriesInList = event.progress.stateOperators(0).customMetrics.get("numListStateWithTTLVars")
      val expiredEntries = event.progress.stateOperators(0).customMetrics.get("numValuesRemovedDueToTTLExpiry")
      println(s"Recorded: ${entriesInList} present and ${expiredEntries} expired")
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

// The code shows that the TTL in the list state applies per elements added; consequently, you should see
// things like 1 element in the list and 0 removed, or 1 element in the list but 3 removed, ...
// Here is a sample output:
// Sending records
//outputMode=Complete / timeMode=ProcessingTime
//outputMode=Complete / timeMode=ProcessingTime
//2025-08-18T12:25:28.229127720Z Executor task launch worker for task 1.0 in stage 0.0 (TID 1) ERROR Recursive call to appender File
//2025-08-18T12:25:28.230478212Z Executor task launch worker for task 1.0 in stage 0.0 (TID 1) ERROR Recursive call to appender File
//2025-08-18T12:25:28.233258598Z Executor task launch worker for task 1.0 in stage 0.0 (TID 1) ERROR Recursive call to appender File
//2025-08-18T12:25:28.342214306Z Executor task launch worker for task 1.0 in stage 0.0 (TID 1) ERROR Recursive call to appender File
//outputMode=Complete / timeMode=ProcessingTime
//Handling new rows for visit1 timer values org.apache.spark.sql.execution.streaming.TimerValuesImpl@467eed9f // 2025-08-18 12:25:26.085
//Appending ExpiredPage(alerts,2025-06-05 10:00:00.0);ExpiredPage(page-1,2025-06-05 10:30:00.0);ExpiredPage(contact,2025-06-05 10:01:00.0)
//-------------------------------------------
//Batch: 0
//-------------------------------------------
//+-----+
//|value|
//+-----+
//+-----+
//
//Recorded: 1 present and 0 expired
//outputMode=Complete / timeMode=ProcessingTime
//Sending records
//-------------------------------------------
//Batch: 1
//-------------------------------------------
//+-----+
//|value|
//+-----+
//+-----+
//
//Recorded: 1 present and 3 expired
//Sending records
//outputMode=Complete / timeMode=ProcessingTime
//Handling new rows for visit1 timer values org.apache.spark.sql.execution.streaming.TimerValuesImpl@1b21a8b3 // 2025-08-18 12:26:00.006
//Appending ExpiredPage(page-2,2025-06-05 10:47:00.0);ExpiredPage(page-3,2025-06-05 10:57:00.0)
//-------------------------------------------
//Batch: 2
//-------------------------------------------
//+-----+
//|value|
//+-----+
//+-----+
//
//Recorded: 1 present and 0 expired
object TransformWithStateAndTtlListExample  {

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.shuffle.partitions", 1).config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    import sparkSession.implicits._
    sparkSession.streams.addListener(ListenerForRemovedEntries)
    val  visitsInputStream = new MemoryStream[Visit](1, sparkSession.sqlContext, Some(2))
    val visitsInput = visitsInputStream.toDS()
    val groupedVisits = visitsInput.groupByKey(row => row.visit_id)

    val sessionsMaker = groupedVisits.transformWithState(
      statefulProcessor=new ExpiredPagesStatefulProcessor(),
      timeMode=TimeMode.ProcessingTime(),
      outputMode=OutputMode.Complete()
    )
    visitsInputStream.addData(Seq(
      Visit.visit("visit1", "10:00:00", "alerts"),  Visit.visit("visit1", "10:01:00", "contact")
    ))
    new Thread(() => {
      Seq(
        Seq(Visit.visit("visit1", "10:30:00", "page-1")),
        Seq(Visit.visit("visit1", "10:47:00", "page-2")),
        Seq(Visit.visit("visit1", "10:57:00", "page-3")),
      ).foreach(pack => {
        println("Sending records")
        visitsInputStream.addData(pack)
        Thread.sleep(15000)
      })

    }).start()

    val query = sessionsMaker.writeStream.trigger(Trigger.ProcessingTime("20 seconds"))
      .format("console").option("truncate", false).start()

    query.awaitTermination()
  }

}

case class ExpiredPage(page: String, visitTime: Timestamp)

class ExpiredPagesStatefulProcessor extends StatefulProcessor[String, Visit, String] {

  @transient private var expirationTimesPerPages: ListState[ExpiredPage] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    println(s"outputMode=${outputMode} / timeMode=${timeMode}")
    expirationTimesPerPages =  getHandle.getListState[ExpiredPage]("expiration_pages", Encoders.product[ExpiredPage],
      TTLConfig(Duration.ofSeconds(10L)))
  }

  override def handleInputRows(visitId: String, visits: Iterator[Visit], timerValues: TimerValues): Iterator[String] = {
    println(s"Handling new rows for ${visitId} timer values ${timerValues} // " +
      s"${new Timestamp(timerValues.getCurrentProcessingTimeInMs())}")
    val visitsWithExpirationTime = visits.map(visit => {
      ExpiredPage(visit.page, visit.event_time)
    }).toArray
    println(s"Appending ${visitsWithExpirationTime.mkString(";")}")
    expirationTimesPerPages.appendList(visitsWithExpirationTime)
    Iterator.empty
  }

  override def handleExpiredTimer(visitId: String, timerValues: TimerValues,
                                  expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
    println(s"State is expiring for ${visitId} and ${new Timestamp(timerValues.getCurrentWatermarkInMs())} for" +
      s" ${new Timestamp(expiredTimerInfo.getExpiryTimeInMs())} ")
    println(s"Expired timer info: ${expiredTimerInfo}")
    Iterator.empty
  }
}
