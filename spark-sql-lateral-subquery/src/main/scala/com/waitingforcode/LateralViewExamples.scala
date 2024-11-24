package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Try}

object LateralViewExamples {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/spark-sql-lateral-view"
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[*]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .enableHiveSupport()
      .getOrCreate()
    FileUtils.deleteDirectory(new File(outputDir))

    sparkSession.sql(
      """CREATE TABLE numbers (
        | nr INT
        |)
        |""".stripMargin)
    sparkSession.sql("INSERT INTO numbers VALUES (1), (5), (10)")

    println("====== Multiply from an exploded list ======")
    sparkSession.sql("SELECT n.nr, n.nr * multipliers.col FROM numbers n LATERAL VIEW EXPLODE(ARRAY(2, 6, 8)) multipliers")
      .explain(true)
    println("====== Multiply from an pos-exploded list ======")
    sparkSession.sql("SELECT multipliers.*, n.nr, n.nr * multipliers.col  FROM numbers n " +
        "LATERAL VIEW POSEXPLODE(ARRAY(2, 6, 8)) multipliers")
      .explain(true)

    println("Trying to see if the LATERAL VIEW works with a SELECT statement...")
    val lateralViewWithSelectOutcome = Try({
      sparkSession.sql(
        """CREATE TABLE multipliers (
          | nr INT,
          | multiplier INT
          |)
          |""".stripMargin)
      sparkSession.sql("INSERT INTO multipliers VALUES (1, 3), (1, 5), (5, 3), (5, 4), (10, 2), (10, 6)")
      sparkSession.sql("SELECT * FROM numbers LATERAL VIEW SELECT multipliers FROM multipliers m").explain(true)
    })
    lateralViewWithSelectOutcome match {
      case Failure(error) => {
        println("An error occurred while using LATERAL VIEW with a SELECT statement:")
        println(error.getMessage)
      }
    }
  }

}
