package com.waitingforcode.batch

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.streaming._

import java.io.File
import java.util.TimeZone

case class StateToPersist(letter: String, numbers: Seq[Long])

object TransformWithStateBatchIncremental  {

  def main(args: Array[String]): Unit = {
    val stateDirectory = "/tmp/wfc/transformwithstate_batch/state"
    FileUtils.deleteDirectory(new File(stateDirectory))
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.shuffle.partitions", 1).config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    import sparkSession.implicits._

    val batchNumbers = Seq(1, 2, 3)
    batchNumbers.foreach(batchNumber => {
      val inputNumbers = Seq(Numbers("a", 1), Numbers("c", 3)).toDS()
      val groupedNumbers = inputNumbers.groupByKey(n => n.letter)

      val concatenatedNumbersWithState = if (batchNumber == batchNumbers.head) {
        groupedNumbers.transformWithState(
          new NumbersProcessorForBatchIncremental(),  TimeMode.None(), OutputMode.Append()
        )
      }  else {
        val initialState = sparkSession.read.json(s"${stateDirectory}/${batchNumber - 1}").as[StateToPersist]
          .groupByKey(n => n.letter)
        groupedNumbers.transformWithState(
          new NumbersProcessorForBatchIncremental(),  TimeMode.None(), OutputMode.Append(), initialState
        )
      }

      concatenatedNumbersWithState.persist()
      concatenatedNumbersWithState.select("_1").show()
      concatenatedNumbersWithState.select("_2.*")
        .write.format("json").save(s"${stateDirectory}/${batchNumber}")
      concatenatedNumbersWithState.unpersist()
    })

  }

}


class NumbersProcessorForBatchIncremental extends
  StatefulProcessorWithInitialState[String, Numbers, (String, StateToPersist), StateToPersist] {

  @transient private var accumulatedNumbers: ListState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    println(s"Initializing state with ${outputMode} and ${timeMode}")
    accumulatedNumbers = getHandle.getListState("numbers", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInitialState(key: String, initialState: StateToPersist, timerValues: TimerValues): Unit = {
    println(s"Handling initial state ${initialState} for ${key} and ${timerValues}")
    accumulatedNumbers.appendList(initialState.numbers.toArray)
  }


  override def handleInputRows(letter: String, numbers: Iterator[Numbers],
                               timerValues: TimerValues): Iterator[(String, StateToPersist)] = {
    numbers.foreach(number => {
      accumulatedNumbers.appendValue(number.number)
    })
    val allNumbers = accumulatedNumbers.get().toSeq
    Iterator.single(
      (letter + " = " + allNumbers.mkString("; "), StateToPersist(letter, allNumbers))
    )
  }

  override def handleExpiredTimer(letter: String, timerValues: TimerValues,
                                  expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, StateToPersist)] = {

    throw new RuntimeException("Expiration shouldn't be handled in the batch transformWithState")
  }
}