package com.waitingforcode

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession, functions}

import java.sql.Timestamp
import java.util.TimeZone

object DropDuplicatesWithinWatermarkForKafka {

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    val kafkaSource = sparkSession.readStream
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("subscribe", "events").format("kafka").load()

    val eventsToDeduplicate = kafkaSource.select(functions.from_json(
      functions.col("value").cast("string"), StructType.fromDDL("id INT, eventTime TIMESTAMP")).alias("value"))
      .selectExpr("value.*")
    
    val query = eventsToDeduplicate
      .withWatermark("eventTime", "20 minutes")
      .dropDuplicatesWithinWatermark("id")

    val writeQuery = query.writeStream.format("console")
      .option("truncate", false).start()

    import sparkSession.implicits._
    Seq(
      (0, """{"id": 1, "eventTime": "2023-06-10 10:21:00"}"""),
      (0, """{"id": 1, "eventTime": "2023-06-10 10:22:00"}"""),
      (0, """{"id": 2, "eventTime": "2023-06-10 10:23:00"}"""),
      (0, """{"id": 3, "eventTime": "2023-06-10 10:24:00"}"""),
      (1, """{"id": 10, "eventTime": "2023-06-10 11:20:00"}"""),
      (1, """{"id": 10, "eventTime": "2023-06-10 11:25:00"}""")
    ).toDF("partition", "value")
      .write.option("kafka.bootstrap.servers", "localhost:9094").option("topic", "events")
      .format("kafka").save()

    writeQuery.processAllAvailable()
    println(writeQuery.lastProgress.prettyJson)
    writeQuery.explain(true)


    Seq(
      (0, """{"id": 4, "eventTime": "2023-06-10 10:25:00"}"""),
      (0, """{"id": 5, "eventTime": "2023-06-10 10:26:00"}"""),
      (0, """{"id": 6, "eventTime": "2023-06-10 10:27:00"}"""),
      (0, """{"id": 7, "eventTime": "2023-06-10 10:28:00"}"""),
      (1, """{"id": 11, "eventTime": "2023-06-10 11:11:00"}"""),
      (1, """{"id": 12, "eventTime": "2023-06-10 11:15:00"}""")
    ).toDF("partition", "value")
      .write.option("kafka.bootstrap.servers", "localhost:9094").option("topic", "events")
      .format("kafka").save()

    writeQuery.processAllAvailable()
    println(writeQuery.lastProgress.prettyJson)
  }

}
