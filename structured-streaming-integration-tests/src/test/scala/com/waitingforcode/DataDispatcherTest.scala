package com.waitingforcode

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.time.Duration
import scala.collection.JavaConverters._
import java.util.Properties

class DataDispatcherTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private var kafkaContainer: KafkaContainer = _
  private lazy val bootstrapServers = kafkaContainer.getBootstrapServers
  private lazy val commonConfiguration = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("auto.offset.reset", "earliest")
    props.setProperty("group.id", "standard_client")
    props
  }
  private val inputTopicName = "input"
  private val validDataTopicName = "topic_valid"
  private val invalidDataTopicName = "topic_invalid"
  private val checkpointLocation = "/tmp/spark-it/checkpoint"

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)


  before {
    println("Starting Kafka")
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
    kafkaContainer.start()

    println("Creating topics")
    val kafkaAdmin = AdminClient.create(commonConfiguration)
    val topicsToCreate = Seq(inputTopicName, invalidDataTopicName, validDataTopicName).par
    topicsToCreate.foreach(topic => {
      kafkaAdmin.createTopics(Seq(new NewTopic(topic, 3, 1.toShort)).asJava)
    })

    println("Sending data")
    val kafkaProducer = new KafkaProducer[String, String](commonConfiguration)
    (0 to 6).foreach(id => kafkaProducer.send(new ProducerRecord[String, String](inputTopicName, s"${id}",
      s"v${id}")))
    kafkaProducer.flush()

    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  after {
    println("Stopping Kafka")
    kafkaContainer.stop()
  }

  "data dispatcher" should "copy invalid records to topic_invalid and valid records to topic_valid" in {
    val dataDispatcherConfig = DataDispatcherConfig(
      bootstrapServers = bootstrapServers,
      inputTopic = InputTopicConfig(name = inputTopicName,
        extra = Map("startingOffsets" -> "EARLIEST", "maxOffsetsPerTrigger" -> "2")),
      validDataTopic = validDataTopicName, invalidDataTopic = invalidDataTopicName,
      checkpointLocation = checkpointLocation,
      isIntegrationTest = true
    )
    val configJson = objectMapper.writeValueAsString(dataDispatcherConfig)

    val dataDispatcher = DataDispatcher.main(Array(configJson))

    val dataConsumer = new KafkaConsumer[String, String](commonConfiguration)
    dataConsumer.subscribe(Seq(validDataTopicName, invalidDataTopicName).asJava)
    val data = dataConsumer.poll(Duration.ofSeconds(30))
    val dispatchedRecords = data.asScala.groupBy(record => record.topic())
      .map {
        case (topic, records) => (topic, records.map(record => (record.key(), record.value())))
      }
    dispatchedRecords(validDataTopicName) should have size 3
    dispatchedRecords(validDataTopicName) should contain allOf (
      ("1", "v1"), ("3", "v3"), ("5", "v5")
    )
    dispatchedRecords(invalidDataTopicName) should have size 4
    dispatchedRecords(invalidDataTopicName) should contain allOf (
      ("0", "v0"), ("2", "v2"), ("4", "v4"), ("6", "v6")
    )
  }

}
