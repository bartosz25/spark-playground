package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import za.co.absa.abris.avro.functions
import za.co.absa.abris.config.AbrisConfig

object ProducerAppV2 extends App {

  val sparkSession = SparkSession.builder()
    .appName("Avro Producer").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()
  import sparkSession.implicits._

  val writeSchema = AbrisConfig.toConfluentAvro.downloadSchemaByLatestVersion
    .andTopicNameStrategy(DemoTopicName, false).usingSchemaRegistry(SchemaRegistryUrl)
  val orders = Seq(
    OrderData3("3", 30.33d, Some(19.9d)), OrderData3("4", 34.45d, Some(19.3d))
  ).toDF()
  val allColumns = struct(orders.columns.head, orders.columns.tail: _*)
  val avroRecordsToWrite = orders.select(functions.to_avro(allColumns, writeSchema).as("value"))

  avroRecordsToWrite
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", KafkaBrokerUlr)
    .option("topic", DemoTopicName)
    .save()

}
case class OrderData3(id: String, amount: Double, vat: Option[Double])