package com.waitingforcode.table

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object JsonTableUpdater extends App {

  FileUtils.delete(new File("/tmp/join_structured_streaming/table/warehouse/metastore_db/db.lck"))
  FileUtils.delete(new File("/tmp/join_structured_streaming/table/warehouse/metastore_db/dbex.lck"))
  val sparkSession = SparkSession.builder().master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
    .getOrCreate()
  import sparkSession.implicits._

  val dataset = Seq(
    (0, "AA"), (1, "BB"), (2, "CC"), (3, "DD")
  ).toDF("number", "letter")

  sparkSession.sql("SELECT * FROM letters_enrichment_table").show(false)

  dataset.write.mode("overwrite").saveAsTable("letters_enrichment_table")
  sparkSession.sql("SELECT * FROM letters_enrichment_table").show(false)

}
