package com.waitingforcode

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.sys.process.Process

object TestDataGenerator extends App {

  val topics = Seq(PerfectUseCaseTopicName, LessPerfectUseCaseTopicName, AnotherLessPerfectUseCaseTopicName)
  topics.foreach(topic => {
    println(s"== Deleting already existing topic ${topic} ==")
    val deleteTopicResult =
      Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --delete").run()
    deleteTopicResult.exitValue()
    println(s"== Creating ${topic} ==")
    val options = if (topic == AnotherLessPerfectUseCaseTopicName) {
      "--config min.compaction.lag.ms=100 --config max.compaction.lag.ms=100 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.001"
    } else {
      ""
    }
    val createTopicResult =
      Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --create --partitions 2 ${options}").run()
    createTopicResult.exitValue()
  })

  // Used to the run before the failure
  val messagesToSend = Seq(
    Map(
      PerfectUseCaseTopicName -> Seq("A", "B", "C", "D"),
      LessPerfectUseCaseTopicName -> Seq("A1", "B1", "C1", "D1"),
      AnotherLessPerfectUseCaseTopicName -> Seq("A2", "B2", "C2", "D2")
    ),
    Map(
      PerfectUseCaseTopicName -> Seq("E", "F", "G", "H"),
      LessPerfectUseCaseTopicName -> Seq("E1", "F1", "G1", "H1"),
      AnotherLessPerfectUseCaseTopicName -> Seq("E2", "F2", "G2", "D2", "H2"),
    ),
    Map(
      PerfectUseCaseTopicName -> Seq("I", "J", "K", "L"),
      LessPerfectUseCaseTopicName -> Seq("I1", "J1"),
      AnotherLessPerfectUseCaseTopicName -> Seq("I2", "J2", "G2")
    ),
    Map(
      PerfectUseCaseTopicName -> Seq("M", "N", "O", "P"),
      LessPerfectUseCaseTopicName -> Seq("K1", "L1", "M1", "N1"),
      AnotherLessPerfectUseCaseTopicName -> Seq("K2", "L2", "M2", "N2")
    )
  )
  import scala.collection.JavaConverters.mapAsJavaMapConverter
  val kafkaProducer = new KafkaProducer[String, String](Map[String, Object](
    "bootstrap.servers" -> "localhost:29092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  ).asJava)
  println("Press enter to send the first message")
  scala.io.StdIn.readLine()
  messagesToSend.foreach(messagesPerTopic => {
    var counter = 0
    messagesPerTopic.foreach {
      case (topic, messages) => {
        messages.foreach(recordToSend => {
          println(s"Sending ${recordToSend}")
          val partition = counter % 2
          kafkaProducer.send(new ProducerRecord[String, String](topic, partition,
            recordToSend, recordToSend))
          counter += 1
        })
      }
    }
    kafkaProducer.flush()
    println("Press enter to send another message")
    scala.io.StdIn.readLine()
  })

  println("All messages were send")
  kafkaProducer.close()
}
