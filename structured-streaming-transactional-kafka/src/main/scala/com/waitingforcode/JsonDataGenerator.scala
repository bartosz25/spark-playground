package com.waitingforcode

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.sys.process.Process

object JsonDataGenerator extends App {

  println("== Deleting already existing topic ==")
  val deleteTopicResult =
    Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${OutputTopicTransactional} --delete").run()
  deleteTopicResult.exitValue()
  println(s"== Creating ${OutputTopicTransactional} ==")
  val createTopicResult =
    Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${OutputTopicTransactional} --create --partitions 2").run()
  createTopicResult.exitValue()

  // Used to the run before the failure
  val messagesToSendRun1 = Seq(
    Seq("A", "B", "C", "D"),
    Seq("E", "F", "G", "H"),
    Seq("I", "J", "K", "L"),
  )
  // Used for the run after the failure
  val messagesToSendRun2 = Seq(
    messagesToSendRun1(2),
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
    messages.foreach(recordToSend => {
      println(s"Sending ${recordToSend}")
      kafkaProducer.send(new ProducerRecord[String, String](InputTopicName, recordToSend))
    })
    kafkaProducer.flush()
    println("Press enter to send another message")
    scala.io.StdIn.readLine()
  })

  println("All messages were send")
  kafkaProducer.close()
}
