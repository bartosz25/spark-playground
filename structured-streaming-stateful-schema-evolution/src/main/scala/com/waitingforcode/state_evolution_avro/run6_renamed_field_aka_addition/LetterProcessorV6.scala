package com.waitingforcode.state_evolution_avro.run6_renamed_field_aka_addition


import com.waitingforcode.state_evolution_avro.Letter
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

case class LetterStateV6(processingTimeAsDouble: Option[Double])

class LetterProcessorV6 extends StatefulProcessor[Int, Letter, String] {

  @transient private var letterStateValue: ValueState[LetterStateV6] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    letterStateValue =  getHandle.getValueState("letterState", Encoders.product[LetterStateV6], TTLConfig.NONE)
  }

  override def handleInputRows(key: Int, inputRows: Iterator[Letter],
                               timerValues: TimerValues): Iterator[String] = {
    val restoredState: Option[LetterStateV6] = Option(letterStateValue.get())
    println(s"Restored state was ${restoredState}")
    val processingTime = Some(timerValues.getCurrentProcessingTimeInMs().toDouble)

    val newState = restoredState.map(stateV1 => {
      stateV1.copy(processingTimeAsDouble = processingTime)
    }).getOrElse(LetterStateV6(processingTimeAsDouble = processingTime))
    letterStateValue.update(newState)

    Iterator(s"${key} = ${letterStateValue.get().toString}")
  }
}
