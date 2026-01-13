package com.waitingforcode.state_evolution_avro.run4_replaced_type_promotion

import com.waitingforcode.state_evolution_avro.Letter
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

/**
 * Type Promotion Rules
 *
 * Beyond adding/removing fields, Avro allows "Type Promotion" where a reader can promote a smaller type to a larger one without error:
 *
 * int → long, float, or double
 *
 * long → float or double
 *
 * float → double
 *
 * string → bytes (and vice versa)
 *
 * @param processingTime
 */
case class LetterStateV4(processingTime: Option[Double])

class LetterProcessorV4 extends StatefulProcessor[Int, Letter, String] {

  @transient private var letterStateValue: ValueState[LetterStateV4] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    letterStateValue =  getHandle.getValueState("letterState", Encoders.product[LetterStateV4], TTLConfig.NONE)
  }

  override def handleInputRows(key: Int, inputRows: Iterator[Letter],
                               timerValues: TimerValues): Iterator[String] = {
    val restoredState: Option[LetterStateV4] = Option(letterStateValue.get())
    println(s"Restored state was ${restoredState}")
    val processingTime = Some(timerValues.getCurrentProcessingTimeInMs().toDouble)

    val newState = restoredState.map(stateV1 => {
      stateV1.copy(processingTime = processingTime)
    }).getOrElse(LetterStateV4(processingTime = processingTime))
    letterStateValue.update(newState)

    Iterator(s"${key} = ${letterStateValue.get().toString}")
  }
}
