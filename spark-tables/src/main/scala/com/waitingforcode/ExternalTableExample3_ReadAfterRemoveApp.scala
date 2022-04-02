package com.waitingforcode

import org.apache.spark.sql.SparkSession

object ExternalTableExample3_ReadAfterRemoveApp extends App {

  val sparkSession = SparkSession.builder()
    .appName("External table reader").master("local[*]")
    .config("spark.sql.warehouse.dir", dataWarehouseExternalTablesDirectory)
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

  // We're working here with external tables, so the data is still there
  // after the DROP TABLE command
  sparkSession.read.parquet(s"${externalDataOutputDir}").show(false)
}
