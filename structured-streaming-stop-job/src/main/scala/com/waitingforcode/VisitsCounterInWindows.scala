package com.waitingforcode

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SparkSession, functions}

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

object VisitsCounterInWindows {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]")
      .config("spark.sql.streaming.stopTimeout", "10000") // 10 seconds
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    val visitsSource = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:9094",
        "subscribe" -> "visits",
        "startingOffsets" -> "LATEST",
        "maxRecordsPerTrigger" -> "15"
      )).load()

    import sparkSession.implicits._

    val aggregationQuery = visitsSource.select(
      functions.from_json($"value".cast("string"), "visit_id STRING", Map.empty[String, String]).as("visit")
    )
      .filter(r => {
        println("...processing a row")
        Thread.sleep(1000L) // just to add some latency and see if the query stops gracefully (after completing the micro-batch)
        true
      })
      .groupBy("visit.visit_id").count()

    val startedAggregationQuery = aggregationQuery.writeStream.outputMode("update")
      .format("console").start()

    new Thread(() => {
      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", "localhost:9094")
      consumerProperties.put("key.deserializer", classOf[StringDeserializer].getName)
      consumerProperties.put("value.deserializer", classOf[StringDeserializer].getName)
      consumerProperties.put("auto.offset.reset", "latest")
      consumerProperties.put("group.id", s"marker-${System.currentTimeMillis()}")
      val kafkaConsumer = new KafkaConsumer[String, String](consumerProperties)
      kafkaConsumer.subscribe(util.Arrays.asList("markers"))
      while (true) {
        val messages = kafkaConsumer.poll(Duration.ofSeconds(5))
        val shouldStopTheJob = messages.records("markers").iterator().hasNext
        if (shouldStopTheJob) {
          println(s"Received a marker, stopping the query now...${startedAggregationQuery.id}")
          while (startedAggregationQuery.status.isTriggerActive) {}
          startedAggregationQuery.stop()
          Thread.currentThread().interrupt()
          Thread.currentThread().join(TimeUnit.SECONDS.toMillis(5))
        } else {
          println(s"Empty record ${messages.count()}")
        }
      }
    }).start()

    /*
    sys.addShutdownHook {
      println(s"SHUTDOWN!!!!!!!! ==> ${startedAggregationQuery.status}")
      while (startedAggregationQuery.status.isTriggerActive) {}
      startedAggregationQuery.stop()
    }*/

    // docker exec -ti wfc_kafka kafka-console-producer.sh --topic markers --bootstrap-server localhost:9092

    sparkSession.streams.awaitAnyTermination()
  }

}
