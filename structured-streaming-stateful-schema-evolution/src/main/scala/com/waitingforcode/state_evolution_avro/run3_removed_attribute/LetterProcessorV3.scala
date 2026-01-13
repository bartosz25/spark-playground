package com.waitingforcode.state_evolution_avro.run3_removed_attribute

import com.waitingforcode.state_evolution_avro.Letter
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

case class LetterStateV3(processingTime: Long)

class LetterProcessorV3 extends StatefulProcessor[Int, Letter, String] {

  @transient private var letterStateValue: ValueState[LetterStateV3] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    letterStateValue =  getHandle.getValueState("letterState", Encoders.product[LetterStateV3], TTLConfig.NONE)
  }

  override def handleInputRows(key: Int, inputRows: Iterator[Letter],
                               timerValues: TimerValues): Iterator[String] = {
    val restoredState: Option[LetterStateV3] = Option(letterStateValue.get())
    println(s"Restored state was ${restoredState}")
    val processingTime = timerValues.getCurrentProcessingTimeInMs()

    val newState = restoredState.map(stateV1 => {
      stateV1.copy(processingTime = processingTime)
    }).getOrElse(LetterStateV3(processingTime = processingTime))
    letterStateValue.update(newState)

    Iterator(s"${key} = ${letterStateValue.get().toString}")
  }
}
