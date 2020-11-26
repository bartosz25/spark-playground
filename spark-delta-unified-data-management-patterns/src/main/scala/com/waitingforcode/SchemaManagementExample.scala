package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import scala.util.Try

object SchemaManagementExample extends App {

  val tableDir = "/tmp/delta-schema-management"
  FileUtils.deleteDirectory(new File(tableDir))
  val localSparkSession: SparkSession = SparkSession.builder()
    .appName("[Delta Lake] Schema management").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  import localSparkSession.implicits._

  val numbers = (0 to 10).map(nr => (nr, s"Number=${nr}")).toDF("value", "label")
  numbers.write.format("delta").mode("overwrite").save(tableDir)

  val tableName = "numbers"
  localSparkSession.sql(s"CREATE TABLE ${tableName} USING DELTA LOCATION '${tableDir}'")
  localSparkSession.sql(s"DESCRIBE TABLE ${tableName}").show(false)

  println("[DEBUG] Let's change some data now!")

  // Let's try now to write a new column to the dataset
  val numbersWithExtraColumn = (0 to 10).map(nr => (nr, s"number=${nr}", nr*2))
    .toDF("value", "label", "value_multiplied_by_2")
  Try {
    numbersWithExtraColumn.write.format("delta").mode("append").save(tableDir)
  }.toEither match {
    case Left(error) => error.printStackTrace()
    case Right(_) => throw new RuntimeException("An exception due to the lack of mergeSchema flag should be thrown")
  }
  println("[DEBUG] Let's redo this operation, but this time with mergeSchema option set to true")
  numbersWithExtraColumn.write.format("delta").mode("append")
    .option("mergeSchema", true).save(tableDir)
  println("[DEBUG] Now, we should see this new column in the dataset")
  localSparkSession.sql(s"DESCRIBE TABLE ${tableName}").show(false)

  // Let's try now to introduce an incompatible change in the schema
  // As you can see, the value field is a string and there is no
  // implicit conversion from a string to an integer
  val stringifiedNumbers = (0 to 10).map(nr => (nr.toString, s"Number=${nr}", nr*2))
    .toDF("value", "label", "value_multiplied_by_2")
  Try {
    stringifiedNumbers.write.format("delta").mode("append").save(tableDir)
  }.toEither match {
    case Left(error) => {
      error.printStackTrace()
    }
    case Right(_) => {
      throw new RuntimeException("An schema error should happen but it didn't")
    }
  }
  println("[DEBUG] Let's redo this operation but this time with overwriteSchema option")
  // Will overwrite everything!!!!!! Cna be a costly operation!
  stringifiedNumbers.write.format("delta")
    // IMPORTANT! Mode has to be overwrite!;
    // TODO demo --> show what happens if commented!
    .mode("overwrite")
    .option("mergeSchema", true).option("overwriteSchema", true)
    .save(tableDir)

  println("[DEBUG] Let's see know the data and the schema")
  localSparkSession.sql(s"DESCRIBE TABLE ${tableName}").show(false)
  localSparkSession.sql(s"SELECT * FROM ${tableName}").show(false)

}