package com.waitingforcode.state_evolution_problem.run2_valid

import com.waitingforcode.state_evolution_problem.Letter
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

case class LetterStateV2(lowerLetters: Seq[String], processingTime: Long)

class LetterProcessorV2 extends StatefulProcessor[Int, Letter, String] {

  @transient private var letterStateValue: ValueState[LetterStateV2] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    letterStateValue =  getHandle.getValueState("letterState", Encoders.product[LetterStateV2], TTLConfig.NONE)
  }

  override def handleInputRows(key: Int, inputRows: Iterator[Letter],
                               timerValues: TimerValues): Iterator[String] = {
    val restoredState: Option[LetterStateV2] = Option(letterStateValue.get())
    println(s"Restored state was ${restoredState}")
    val newLowerLetters = inputRows.map(letter => letter.lowerCase).toSeq
    val processingTime = timerValues.getCurrentProcessingTimeInMs()

    val newState = restoredState.map(stateV1 => {
      stateV1.copy(processingTime = processingTime, lowerLetters = stateV1.lowerLetters ++ newLowerLetters)
    }).getOrElse(LetterStateV2(processingTime = processingTime, lowerLetters = newLowerLetters))
    letterStateValue.update(newState)

    Iterator(s"${key} = ${letterStateValue.get().toString}")
  }
}
