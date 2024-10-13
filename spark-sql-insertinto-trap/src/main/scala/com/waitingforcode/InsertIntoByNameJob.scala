package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object InsertIntoByNameJob {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/spark-sql-insertinto-trap"
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .enableHiveSupport()
      .getOrCreate()
    FileUtils.deleteDirectory(new File(outputDir))

    sparkSession.sql(
      """
        |CREATE TABLE numbers_with_letters (
        | lower_case STRING,
        | upper_case STRING,
        | nr INT
        |)
        |""".stripMargin)

    println("Inserting records 'e' and 'f' with incorrect order and type mismatch...")
    sparkSession.sql(
      """
        |INSERT INTO numbers_with_letters (upper_case, lower_case, nr) VALUES
        |('E', 'e', 5),
        |('F', 'f', 6)
        |""".stripMargin)

    sparkSession.sql("SELECT * FROM numbers_with_letters ORDER BY nr ASC").show(truncate=false)
    println("...done")
  }

}
