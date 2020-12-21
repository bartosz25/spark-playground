package com

package object waitingforcode {

  val InputTopicName = "kafka_demo_input"
  val OutputTopicNonTransactional = "non_transactional_kafka_demo_output"
  val OutputTopicTransactional = "transactional_kafka_demo_output"
  val OutputDirCheckpoint = s"/tmp/waitingforcode/${OutputTopicTransactional}/checkpoint"

}
