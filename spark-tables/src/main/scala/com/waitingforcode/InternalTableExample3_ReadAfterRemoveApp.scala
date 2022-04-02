package com.waitingforcode

import org.apache.spark.sql.SparkSession

object InternalTableExample3_ReadAfterRemoveApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("Internal table reader").master("local[*]")
    .config("spark.sql.warehouse.dir", dataWarehouseInternalTablesDirectory)
    .enableHiveSupport()
    .getOrCreate()

  // Let's first remove one of the internal tables
  sparkSession.sql("DROP TABLE letters_saveastable")

  // Check if the table data is there
  try {
    sparkSession.sql("SELECT * FROM letters_saveastable").show(false)
  } catch {
    case e => e.printStackTrace()
  }

  // We're working here with managed internal tables, so the DROP also removes the data
  try {
    sparkSession.read.parquet(s"${dataWarehouseInternalTablesDirectory}/letters_saveastable").show(false)
  } catch {
    case e => e.printStackTrace()
  }
}
