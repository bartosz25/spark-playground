package com

package object waitingforcode {

  val InputTopicName = "kafka_demo_input"
  val OutputTopicNonTransactional = "non_transactional_kafka_demo_output"
  val OutputTopicTransactional = "transactional_kafka_demo_output"
  val OutputDirCheckpointTransactional = s"/tmp/waitingforcode/${OutputTopicTransactional}/checkpoint"
  val OutputDirCheckpointNonTransactional = s"/tmp/waitingforcode/${OutputTopicNonTransactional}/checkpoint"

  val ShouldFailOnK = false
}
