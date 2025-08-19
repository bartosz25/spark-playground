package com.waitingforcode.state_init

import org.apache.spark.sql.classic.KeyValueGroupedDataset
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Encoders, SparkSession}

import java.sql.Timestamp
import java.util.TimeZone

case class Numbers(letter: String, number: Int)

object TransformWithStateInitStateWithReconciliation  {

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.shuffle.partitions", 1).config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    import sparkSession.implicits._

    val initialState = Seq(
      Numbers("a", 1), Numbers("b", 2), Numbers("a", 1)
    ).toDS().groupByKey(r => r.letter)

    val visitsInputStream = new MemoryStream[Numbers](1, sparkSession.sqlContext, Some(2))
    val visitsInput = visitsInputStream.toDS()
    val groupedVisits: KeyValueGroupedDataset[String, Numbers] = visitsInput.groupByKey(row => row.letter)

    val sessionsMaker = groupedVisits.transformWithState(
      statefulProcessor=new TransformWithStateInitStateWithReconciliationSessionProcessor(),
      timeMode=TimeMode.None(),
      outputMode=OutputMode.Update(),
      initialState = initialState
    )

    val query = sessionsMaker.writeStream.format("console").option("truncate", false).start()

    visitsInputStream.addData(Seq(
      Numbers("a", 1), Numbers("c", 3)
    ))
    query.processAllAvailable()

    visitsInputStream.addData(Seq(
      Numbers("b", 2), Numbers("d", 4)
    ))
    query.processAllAvailable()

  }

}

class TransformWithStateInitStateWithReconciliationSessionProcessor extends
  StatefulProcessorWithInitialState[String, Numbers, String, Numbers] {

  @transient private var accumulatedNumbers: ListState[Int] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    println(s"Initializing state with ${outputMode} and ${timeMode}")
    accumulatedNumbers = getHandle.getListState("numbers", Encoders.scalaInt, TTLConfig.NONE)
  }

  override def handleInitialState(key: String, initialState: Numbers, timerValues: TimerValues): Unit = {
    println(s"Handling initial state ${initialState} for ${key} and ${timerValues}")
    accumulatedNumbers.appendValue(initialState.number)
  }


  override def handleInputRows(letter: String, numbers: Iterator[Numbers], timerValues: TimerValues): Iterator[String] = {
    numbers.foreach(number => {
      accumulatedNumbers.appendValue(number.number)
    })
    Iterator.single(
      letter+" = "+accumulatedNumbers.get().mkString("; ")
    )
  }

  override def handleExpiredTimer(letter: String, timerValues: TimerValues,
                                  expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
    println(s"State is expiring for ${letter} and ${new Timestamp(timerValues.getCurrentWatermarkInMs())} for" +
      s" ${new Timestamp(expiredTimerInfo.getExpiryTimeInMs())} ")
    println(s"Expired timer info: ${expiredTimerInfo}")
    Iterator.empty
  }
}