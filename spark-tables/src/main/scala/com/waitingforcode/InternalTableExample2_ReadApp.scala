package com.waitingforcode

import org.apache.spark.sql.SparkSession

object InternalTableExample2_ReadApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("Internal table reader").master("local[*]")
    .config("spark.sql.warehouse.dir", dataWarehouseInternalTablesDirectory)
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.catalog.listTables().toDF().show(false)
  sparkSession.sql("SELECT * FROM letters_saveastable").show(false)
  sparkSession.sql("SELECT * FROM letters_createtable").show(false)
  sparkSession.sql("SELECT * FROM letters_like").show(false)


}
