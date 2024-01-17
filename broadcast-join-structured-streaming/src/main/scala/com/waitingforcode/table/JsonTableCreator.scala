package com.waitingforcode.table

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object JsonTableCreator extends App {

  FileUtils.deleteDirectory(new File(outputDir))

  val sparkSession = SparkSession.builder().master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
    .getOrCreate()
  import sparkSession.implicits._

  val dataset = Seq(
    (0, "A"), (1, "B"), (2, "C")
  ).toDF("number", "letter")

  dataset.write.saveAsTable("letters_enrichment_table")

}
