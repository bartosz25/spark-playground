package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object ExternalTableExample1_CreateApp extends App {
  FileUtils.deleteDirectory(new File(dataWarehouseBaseDir))

  val sparkSession = SparkSession.builder()
    .appName("Internal table creator").master("local[*]")
    .config("spark.sql.warehouse.dir", dataWarehouseExternalTablesDirectory)
    .enableHiveSupport()
    .getOrCreate()
  import sparkSession.implicits._

  val data = Seq(
    Letter("a", "A", Seq("apple", "atlas")),
    Letter("b", "B", Seq("banana", "bean", "bat", "boat")),
    Letter("c", "C", Seq("chocolate", "cocoa"))
  ).toDF

  sparkSession.catalog.listTables().toDF().show(false)
  data.write.mode(SaveMode.Overwrite).format("parquet").save(externalDataOutputDir)

  data.write.mode(SaveMode.Overwrite).option("path", externalDataOutputDir).saveAsTable("letters_saveastable")

  data.createTempView("temp_letters")
  sparkSession.sql(
    s"""
      |CREATE EXTERNAL TABLE letters_createtable (
      |  upper STRING,
      |  lower STRING,
      |  wordsExamples ARRAY<STRING>
      |) STORED AS PARQUET LOCATION '${externalDataOutputDir}'
      |""".stripMargin) // It's Hive format with STORED AS ==> https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-table.html#create-table-with-hive-format

  sparkSession.sql(
    s"""
      |CREATE TABLE letters_like LIKE temp_letters LOCATION '${externalDataOutputDir}'
      |""".stripMargin) // TODO: here the location marks the table as extenral, there is no 'external' keyword https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-like.html

  sparkSession.catalog.listTables().toDF().show(false)

}
