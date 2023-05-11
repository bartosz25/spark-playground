package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.protobuf.functions.{from_protobuf, to_protobuf}

import java.io.File
import java.util.UUID

object ProtobufSupport {

  def main(args: Array[String]): Unit = {
    val mainPath = "/tmp/spark/3.4.0/protobuf"
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    val protoSchema =
      """syntax  = "proto3";
        |
        |message TestEvent {
        |     string id = 1;
        |     string content = 2;
        |}""".stripMargin
    val schemaPath = s"${mainPath}/protobuf.schema"
    FileUtils.writeStringToFile(new File(schemaPath), protoSchema, "UTF-8")

    val descriptorPath = s"${mainPath}/protobuf.pb"
    // To generate the descriptor file, you must install the `protoc` library
    // and call:
    // protoc --descriptor_set_out=/tmp/spark/3.4.0/protobuf/protobuf.pb --proto_path=/tmp/spark/3.4.0/protobuf protobuf.schema
    // If you want, you can use the file available in the resources folder

    import sparkSession.implicits._
    val datasetWithProtobuf = Seq(
      (UUID.randomUUID().toString, """{some event data}"""),
      (UUID.randomUUID().toString, """{other event data}""")
    ).toDF("id", "content")
      .select(to_protobuf(
        data = struct($"id", $"content"),
        messageName = "TestEvent",
        descFilePath = descriptorPath).as("value"))
    datasetWithProtobuf.show(false)

    datasetWithProtobuf.select(
      from_protobuf(
        data = $"value",
        messageName = "TestEvent",
        descFilePath = descriptorPath
      )
    ).show(false)
  }
}
