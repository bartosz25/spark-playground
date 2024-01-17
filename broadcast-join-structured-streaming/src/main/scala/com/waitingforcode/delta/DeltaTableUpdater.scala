package com.waitingforcode.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object DeltaTableUpdater extends App {

  FileUtils.delete(new File("/tmp/join_structured_streaming/table/warehouse/metastore_db/db.lck"))
  FileUtils.delete(new File("/tmp/join_structured_streaming/table/warehouse/metastore_db/dbex.lck"))
  val sparkSession = SparkSession.builder().master("local[*]")
    .enableHiveSupport()
    .withExtensions(new DeltaSparkSessionExtension())
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
    .getOrCreate()
  import sparkSession.implicits._

  val dataset = Seq(
    (0, "AA"), (1, "BB"), (2, "CC"), (3, "DD")
  ).toDF("number", "letter")

  sparkSession.sql("SELECT * FROM letters_enrichment_table").show(false)

  dataset.write.format("delta").mode("overwrite").saveAsTable("letters_enrichment_table")
  sparkSession.sql("SELECT * FROM letters_enrichment_table").show(false)

}
