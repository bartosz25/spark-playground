package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object SaveAsTableExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/spark-sql-saveastable"
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[1]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .config("spark.sql.sources.useV1SourceList", "")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._
    FileUtils.deleteDirectory(new File(outputDir))

    sparkSession.sql(
      """
        |CREATE TABLE numbers_with_letters (
        | lower_case STRING,
        | upper_case STRING,
        | nr INT
        |) USING DELTA
        |""".stripMargin)

    println("Let's create the DataFrame to insert with the columns ordered differently than the columns declared in the table")

    Seq((3, "C", "c"), (4, "D", "d")).toDF("nr", "upper_case", "lower_case").write
      .mode(SaveMode.Append).format("delta")
      .saveAsTable("numbers_with_letters")

    println("As you can see here, despite wrong order, the data should be correctly added with the per column-resolution")
    sparkSession.sql("SELECT * FROM numbers_with_letters ORDER BY nr ASC").show(truncate=false)
  }

}
