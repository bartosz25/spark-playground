package com.waitingforcode.state_evolution_problem

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

case class LetterStateV1(lowerLetters: Seq[String])

class LetterProcessorV1 extends StatefulProcessor[Int, Letter, String] {

  @transient private var letterStateValue: ValueState[LetterStateV1] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    letterStateValue =  getHandle.getValueState("letterState", Encoders.product[LetterStateV1], TTLConfig.NONE)
  }

  override def handleInputRows(key: Int, inputRows: Iterator[Letter],
                               timerValues: TimerValues): Iterator[String] = {
    val restoredState: Option[LetterStateV1] = Option(letterStateValue.get())
    println(s"Restored state was ${restoredState}")
    val newLowerLetters = inputRows.map(letter => letter.lowerCase).toSeq

    val newState = restoredState.map(stateV1 => {
      stateV1.copy(lowerLetters = stateV1.lowerLetters ++ newLowerLetters)
    }).getOrElse(LetterStateV1(lowerLetters = newLowerLetters))
    letterStateValue.update(newState)

    // expiration time left off for now
    // TODO: programming tip     Iterator(letterStateValue.get().toString) vs
    //     Iterator(
    //      letterStateValue.get().toString
  //      ) It might look beautiful but it makes the program unnecesaraly long
    Iterator(s"${key} = ${letterStateValue.get().toString}")
  }
}
