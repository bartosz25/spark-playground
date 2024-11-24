package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Try}

object LateralSubqueryExamples {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/spark-sql-lateral-subquery"
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
    sparkSession.sql("SELECT n.nr, n.nr * multipliers.col FROM numbers n, " +
        "LATERAL (SELECT EXPLODE(ARRAY(2, 6, 8))) multipliers ORDER BY nr ASC")
      .show(true)
    println("====== Multiply from an pos-exploded list ======")
    sparkSession.sql("SELECT multipliers.*, n.nr, n.nr * multipliers.col FROM numbers n, " +
        "LATERAL (SELECT POSEXPLODE(ARRAY(2, 6, 8))) multipliers ORDER BY nr ASC")
      .show(true)

    // Let's create now the multipliers table and see if it could be used as a correlated subquery
    // Each nr from the numbers table has 2 multipliers
    sparkSession.sql(
      """CREATE TABLE multipliers (
        | nr INT,
        | multiplier INT
        |)
        |""".stripMargin)
    sparkSession.sql("INSERT INTO multipliers VALUES (1, 3), (1, 5), (5, 3), (5, 4), (10, 2), (10, 6)")
    println("Trying correlated subquery")
    val multiplicationFromCorrelatedSubquery = Try(
      sparkSession.sql("SELECT numbers.*, " +
          "(SELECT multiplier FROM multipliers WHERE numbers.nr = multipliers.nr) FROM numbers")
        .show(false)
    )
    multiplicationFromCorrelatedSubquery match {
      case Failure(error) => {
        print("An error occurred while using correlated subquery:")
        println(error.getMessage)
    }
    }

    println("Trying the multiplication from a table with lateral subquery")
    sparkSession.sql("SELECT numbers.*, m.multiplier " +
        "FROM numbers, LATERAL (SELECT multiplier FROM multipliers WHERE numbers.nr = multipliers.nr) m")
      .show(false)
  }

}
