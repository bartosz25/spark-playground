package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object InternalTableExample1_CreateApp extends App {
  FileUtils.deleteDirectory(new File(dataWarehouseBaseDir))

  val sparkSession = SparkSession.builder()
    .appName("Internal table creator").master("local[*]")
    .config("spark.sql.warehouse.dir", dataWarehouseInternalTablesDirectory)
    .enableHiveSupport()
    .getOrCreate()
  import sparkSession.implicits._

  val data = Seq(
    Letter("a", "A", Seq("apple", "atlas")),
    Letter("b", "B", Seq("banana", "bean", "bat", "boat")),
    Letter("c", "C", Seq("chocolate", "cocoa"))
  ).toDF

  sparkSession.catalog.listTables().toDF().show(false)

  data.write.saveAsTable("letters_saveastable")

  data.createTempView("temp_letters")
  sparkSession.sql(
    """
      |CREATE TABLE letters_createtable (
      |  upper STRING,
      |  lower STRING,
      |  wordsExamples ARRAY<STRING>
      |)
      |""".stripMargin)
  sparkSession.sql("INSERT INTO letters_createtable SELECT * FROM temp_letters")


  sparkSession.sql(
    """
      |CREATE TABLE letters_like LIKE temp_letters
      |""".stripMargin)
  sparkSession.sql("INSERT INTO letters_like SELECT * FROM temp_letters")

  sparkSession.catalog.listTables().toDF().show(false)

}
