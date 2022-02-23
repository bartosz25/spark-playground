package com.waitingforcode

case class DataDispatcherConfig(bootstrapServers: String, inputTopic: InputTopicConfig,
                                validDataTopic: String,
                                invalidDataTopic: String,
                                checkpointLocation: String,
                                isIntegrationTest: Boolean = false)

case class InputTopicConfig(name: String, extra: Map[String, String] = Map.empty)