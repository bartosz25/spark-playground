package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object InsertIntoTrapJob {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/spark-sql-insertinto-trap"
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[1]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
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
        |)
        |""".stripMargin)

    // Let's insert first records
    println("Inserting records 'a' and 'b' with correct order...")
    // Seq(("a", "A", 1), ("b", "B", 2)).toDF("lower_case", "upper_case", "nr").write.format("delta")
    //  .insertInto("numbers_with_letters")

    //sparkSession.sql("SELECT * FROM numbers_with_letters ORDER BY nr ASC").show(truncate=false)
    println("...done")

    println("Inserting records 'c' and 'd' with incorrect order...")
    Seq(("C", "c", 3), ("D", "d", 4)).toDF("upper_case", "lower_case", "nr").write.format("delta")
      .insertInto("numbers_with_letters")

    sparkSession.sql("SELECT * FROM numbers_with_letters ORDER BY nr ASC").show(truncate=false)
    println("...done")


    println("Inserting records 'e' and 'f' with incorrect order and type mismatch...")
    //Seq((5, "e", "E"), (6, "f", "F")).toDF("nr", "lower_case", "upper_case").write.format("delta")
    //  .insertInto("numbers_with_letters")

    //sparkSession.sql("SELECT * FROM numbers_with_letters ORDER BY nr ASC").show(truncate=false)
    println("...done")
  }

}
