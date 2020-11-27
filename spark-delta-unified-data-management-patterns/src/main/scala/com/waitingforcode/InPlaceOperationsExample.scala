package com.waitingforcode

import java.io.File

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object InPlaceOperationsExample extends App {

  val outputDir = "/tmp/delta-in-place"
  FileUtils.deleteDirectory(new File(outputDir))
  val localSparkSession: SparkSession = SparkSession.builder()
    .appName("[Delta Lake] In-place changes").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  import localSparkSession.implicits._

  val numberPairs = (1 to 10).map(nr => (nr, nr*2)).toDF("nr", "nr_multiplied_by_2")

  numberPairs.write.format("delta").mode("overwrite").save(outputDir)
  println("Before the operations")
  localSparkSession.read.format("delta").load(outputDir).sort("nr").show(false)
  // Now, let's make a change
  println("After the operations")
  val deltaTable = DeltaTable.forPath(localSparkSession, outputDir)
  deltaTable.delete("nr > 6")
  deltaTable.updateExpr("nr < 3",
    Map("nr_multiplied_by_2" -> "nr_multiplied_by_2 + 1"))


  localSparkSession.read.format("delta").load(outputDir).sort("nr")
    .show(false)
}