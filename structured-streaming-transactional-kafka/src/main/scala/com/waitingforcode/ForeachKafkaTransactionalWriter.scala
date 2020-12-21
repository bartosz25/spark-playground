package com.waitingforcode

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.ForeachWriter

import java.util.Properties
import scala.collection.mutable

class ForeachKafkaTransactionalWriter(outputTopic: String) extends ForeachWriter[String] {

  private var kafkaProducer: KafkaProducer[String, String] = _
  private var initiatedTransactions = false

  override def open(partitionId: Long, epochId: Long): Boolean = {
    kafkaProducer = ForeachKafkaTransactionalWriter.getOrCreate(partitionId)
    if (!initiatedTransactions) {
      kafkaProducer.initTransactions()
      initiatedTransactions = true
    }
    kafkaProducer.beginTransaction()
    true
  }

  override def process(value: String): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](outputTopic, value))
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull!= null) {
      kafkaProducer.abortTransaction()
      kafkaProducer.close()
    } else {
      kafkaProducer.commitTransaction()
    }
  }
}

object ForeachKafkaTransactionalWriter {

  private val producers = mutable.HashMap[Long, KafkaProducer[String, String]]()

  def getOrCreate(partitionId: Long): KafkaProducer[String, String] = {
    producers.getOrElseUpdate(partitionId, create(partitionId))
  }

  private def create(partitionId: Long): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.setProperty("transactional.id", s"transactional_writer_${partitionId}")
    properties.setProperty("bootstrap.servers", "localhost:29092")

    new KafkaProducer[String, String](properties)
  }

}