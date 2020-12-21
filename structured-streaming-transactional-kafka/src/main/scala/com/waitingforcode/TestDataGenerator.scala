package com.waitingforcode

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.sys.process.Process

object TestDataGenerator extends App {

  val topics = Seq(OutputTopicTransactional, OutputTopicNonTransactional, InputTopicName)
  topics.foreach(topic => {
    println(s"== Deleting already existing topic ${topic} ==")
    val deleteTopicResult =
      Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --delete").run()
    deleteTopicResult.exitValue()
    println(s"== Creating ${topic} ==")
    val createTopicResult =
      Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --create --partitions 2").run()
    createTopicResult.exitValue()
  })

  // Used to the run before the failure
  val messagesToSendRun1 = Seq(
    Seq("A", "B", "C", "D"),
    Seq("E", "F", "G", "H"),
    Seq("I", "J", "K", "L"),
    Seq("M", "N", "O", "P")
  )
  import scala.collection.JavaConverters.mapAsJavaMapConverter
  val kafkaProducer = new KafkaProducer[String, String](Map[String, Object](
    "bootstrap.servers" -> "localhost:29092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  ).asJava)
  println("Press enter to send the first message")
  scala.io.StdIn.readLine()
  val messagesToSend = messagesToSendRun1
  messagesToSend.foreach(messages => {
    var counter = 0
    messages.foreach(recordToSend => {
      println(s"Sending ${recordToSend}")
      val partition = counter % 2
      kafkaProducer.send(new ProducerRecord[String, String](InputTopicName, partition,
        s"dummy ${recordToSend} ${System.currentTimeMillis()}", recordToSend))
      counter += 1
    })
    kafkaProducer.flush()
    println("Press enter to send another message")
    scala.io.StdIn.readLine()
  })

  println("All messages were send")
  kafkaProducer.close()
}
