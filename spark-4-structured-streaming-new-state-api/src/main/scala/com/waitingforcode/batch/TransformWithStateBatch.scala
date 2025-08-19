package com.waitingforcode.batch

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.streaming._

import java.sql.Timestamp
import java.util.TimeZone

object TransformWithStateBatch  {

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

    val inputNumbers = Seq(Numbers("a", 1), Numbers("c", 3)).toDS()

    val groupedNumbers = inputNumbers.groupByKey(n => n.letter)

    val batchProcessorForNumbers = groupedNumbers.transformWithState(
      new NumbersProcessorForBatch(),  TimeMode.None(), OutputMode.Append(), initialState
    )

    batchProcessorForNumbers.show()
  }

}

class NumbersProcessorForBatch extends
  StatefulProcessorWithInitialState[String, Numbers, String, Numbers] {

  @transient private var accumulatedNumbers: ListState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    println(s"Initializing state with ${outputMode} and ${timeMode}")
    accumulatedNumbers = getHandle.getListState("numbers", Encoders.scalaLong, TTLConfig.NONE)
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