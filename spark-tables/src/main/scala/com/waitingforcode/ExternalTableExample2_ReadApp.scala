package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ExternalTableExample2_ReadApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("External table reader").master("local[*]")
    .config("spark.sql.warehouse.dir", dataWarehouseExternalTablesDirectory)
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.catalog.listTables().toDF().show(false)
  sparkSession.sql("SELECT * FROM letters_saveastable").show(false)
  sparkSession.sql("SELECT * FROM letters_createtable").show(false)
  sparkSession.sql("SELECT * FROM letters_like").show(false)


}
