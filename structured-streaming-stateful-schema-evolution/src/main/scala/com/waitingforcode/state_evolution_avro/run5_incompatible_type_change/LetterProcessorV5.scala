package com.waitingforcode.state_evolution_avro.run5_incompatible_type_change

import com.waitingforcode.state_evolution_avro.Letter
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

import java.sql.Timestamp

case class LetterStateV5(processingTime: Timestamp)

class LetterProcessorV5 extends StatefulProcessor[Int, Letter, String] {

  @transient private var letterStateValue: ValueState[LetterStateV5] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    letterStateValue =  getHandle.getValueState("letterState", Encoders.product[LetterStateV5], TTLConfig.NONE)
  }

  override def handleInputRows(key: Int, inputRows: Iterator[Letter],
                               timerValues: TimerValues): Iterator[String] = {
    val restoredState: Option[LetterStateV5] = Option(letterStateValue.get())
    println(s"Restored state was ${restoredState}")
    val processingTime = new Timestamp(timerValues.getCurrentProcessingTimeInMs())

    val newState = restoredState.map(stateV1 => {
      stateV1.copy(processingTime = processingTime)
    }).getOrElse(LetterStateV5(processingTime = processingTime))
    letterStateValue.update(newState)

    Iterator(s"${key} = ${letterStateValue.get().toString}")
  }
}
