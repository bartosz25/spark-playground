package com.waitingforcode

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.ForeachWriter

import java.util.Properties
import scala.collection.mutable

class ForeachKafkaTransactionalWriter(outputTopic: String) extends ForeachWriter[String] {

  private var kafkaProducer: KafkaProducer[String, String] = _
  private var skipMicroBatch = false
  private var partitionId = -1L
  private var epochId = -1L

  override def open(partitionId: Long, epochId: Long): Boolean = {
    println(s"Opening foreach for ${partitionId} and ${epochId}")
    skipMicroBatch = epochId <= CommittedTransactionsStore.getLastCommitForPartition(partitionId)
    if (!skipMicroBatch) {
      this.partitionId = partitionId
      this.epochId = epochId
      kafkaProducer = ForeachKafkaTransactionalWriter.getOrCreate(partitionId)
      if (!ForeachKafkaTransactionalWriter.wasInitialized(partitionId)) {
        kafkaProducer.initTransactions()
        ForeachKafkaTransactionalWriter.setInitialized(partitionId)
      }
      kafkaProducer.beginTransaction()
      true
    } else {
      // False indicates that the partition should be skipped
      false
    }
  }

  override def process(value: String): Unit = {
    println(s"Processing ${value}")
    if (value == "K" && ShouldFailOnK) {
      throw new RuntimeException("Got letter that stops the processing")
    }
    kafkaProducer.send(new ProducerRecord[String, String](outputTopic, value))
  }

  override def close(errorOrNull: Throwable): Unit = {
    // Check this too because the close is called even if
    // the open returns true
    if (!skipMicroBatch) {
      if (errorOrNull != null) {
        println("An error occurred, aborting the transaction!")
        kafkaProducer.abortTransaction()
        kafkaProducer.close()
      } else {
        println("Committing the transaction")
        kafkaProducer.commitTransaction()
        CommittedTransactionsStore.commitTransaction(partitionId, epochId)
      }
    }
  }
}

object ForeachKafkaTransactionalWriter {

  private val producers = mutable.HashMap[Long, KafkaProducer[String, String]]()
  private val producersInitialized = mutable.HashMap[Long, Boolean]()

  def getOrCreate(partitionId: Long): KafkaProducer[String, String] = {
    // TODO: find a more Scalaistic way
    val producer = producers.get(partitionId)
    if (producer.isDefined) {
      println("Getting defined producer")
      producer.get
    } else {
      println("Getting new producer")
      val newProducer = create(partitionId)
      producers.put(partitionId, newProducer)
      newProducer
    }
  }

  def wasInitialized(partitionId: Long) = producersInitialized.getOrElse(partitionId, false)
  def setInitialized(partitionId: Long) = producersInitialized.put(partitionId, true)

  private def create(partitionId: Long): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.setProperty("transactional.id", s"transactional_writer_${partitionId}")
    properties.setProperty("bootstrap.servers", "localhost:29092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](properties)
  }

}