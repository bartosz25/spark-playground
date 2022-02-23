package com.waitingforcode

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object DataDispatcher {

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]) = {
    val dataDispatcherConfig = objectMapper.readValue(args(0), classOf[DataDispatcherConfig])

    val sparkSession = SparkSession.builder()
      .appName("Data dispatcher").master("local[*]")
      .getOrCreate()

    val inputKafkaRecords = getInputTopicDataFrame(sparkSession, dataDispatcherConfig)
      .selectExpr("CAST(CAST(key AS STRING) AS INT)", "CAST(value AS STRING)")

    // It's a dummy logic. Obviously a production code could be more complex
    // and the business logic functions should be then unit tested.
    val dataWithTopicName = inputKafkaRecords.withColumn("topic", functions.when(
      inputKafkaRecords("key") % 2 === 0, dataDispatcherConfig.invalidDataTopic
    ).otherwise(dataDispatcherConfig.validDataTopic))
    .selectExpr("CAST(key AS STRING)", "value", "topic")

    val writeQuery = dataWithTopicName
      .writeStream
      .option("checkpointLocation", dataDispatcherConfig.checkpointLocation)
      .format("kafka")
      .option("kafka.bootstrap.servers", dataDispatcherConfig.bootstrapServers)

    if (dataDispatcherConfig.isIntegrationTest) {
      writeQuery.start().awaitTermination(30000L)
    } else {
      writeQuery.start().awaitTermination()
    }
  }

  private def getInputTopicDataFrame(sparkSession: SparkSession, dataDispatcherConfig: DataDispatcherConfig): DataFrame = {
    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dataDispatcherConfig.bootstrapServers)
      .option("subscribe", dataDispatcherConfig.inputTopic.name)
      .options(dataDispatcherConfig.inputTopic.extra)
      .load()
  }

}
