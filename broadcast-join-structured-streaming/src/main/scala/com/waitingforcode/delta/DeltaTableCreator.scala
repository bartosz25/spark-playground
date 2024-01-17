package com.waitingforcode.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object DeltaTableCreator extends App {

  FileUtils.deleteDirectory(new File(outputDir))

  val sparkSession = SparkSession.builder().master("local[*]")
    .enableHiveSupport()
    .withExtensions(new DeltaSparkSessionExtension())
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
    .getOrCreate()
  import sparkSession.implicits._

  val dataset = Seq(
    (0, "A"), (1, "B"), (2, "C")
  ).toDF("number", "letter")

  dataset.write.format("delta").saveAsTable("letters_enrichment_table")

}
