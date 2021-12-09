package com.waitingforcode

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkException
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class AvroSchemaTest extends AnyFlatSpec with Matchers {

  it should "accept schema from a URI" in {
    val schemaPath = getClass.getResource("/user_avro_schema.avsc").getPath
    val schema = new Schema.Parser().parse(new File(schemaPath))
    val user1 = new GenericRecordBuilder(schema)
      .set("first_name", "Bartosz")
      .set("age", 34)
      .build()
    val avroWriter = new DataFileWriter(new GenericDatumWriter[GenericRecord](schema))
    avroWriter.create(schema, new File("/tmp/users.avro"))
    avroWriter.append(user1)
    avroWriter.close()

    val schemaPathWithInvalidField = getClass.getResource("/user_avro_schema_invalid.avsc").getPath
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34416 : Avro schema at reading").master("local[*]")
      .getOrCreate()

    // The test should fail meaning that we correctly loaded the Avro schema from the file
    val error = intercept[SparkException] {
      sparkSession.read.option("avroSchemaUrl", schemaPathWithInvalidField).format("avro").load("/tmp/users.avro")
        .show(false)
    }
    error.getMessage should include("Found user, expecting user, missing required field some_age")
  }

  it should "accept position-based schema" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34365 : Position-based schema resolution").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val outputDir = "/tmp/avro-position-schema"
    (0 to 5).map(nr => (nr, s"nr=${nr}")).toDF("id", "label")
      .write.format("avro").mode(SaveMode.Overwrite).save(outputDir)

    val schemaOnRead = new StructType().add("label", IntegerType).add("id", StringType)

    val positionBasedRows = sparkSession.read.schema(schemaOnRead).option("positionalFieldMatching", true)
      .format("avro").load(outputDir).map(row => s"${row.getAs[Int]("label")}=${row.getAs[String]("id")}")
      .collect()
    positionBasedRows should contain allOf("0=nr=0", "1=nr=1", "2=nr=2", "3=nr=3", "4=nr=4", "5=nr=5")
    val schemaError = intercept[SparkException] {
      sparkSession.read.schema(schemaOnRead).option("positionalFieldMatching", false)
        .format("avro").load(outputDir).show(false)
    }
    schemaError.getMessage should include(
      """IncompatibleSchemaException: Cannot convert Avro type {"type":"record","name":"topLevelRecord","fields":[{"name":"id","type":"int"},{"name":"label","type":["string","null"]}]} to SQL type STRUCT<`label`: INT, `id`: STRING>.""")
  }

}
