package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Try}

object UserDefinedFunctionSQLExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/spark-sql-lateral-subquery"
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[*]")
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

    sparkSession.sql("CREATE FUNCTION multiply_by_2(nr INT) RETURNS INT RETURN nr * 2")
    sparkSession.sql("SELECT multiply_by_2(nr) FROM numbers").show()

    sparkSession.sql("CREATE FUNCTION multipliers() RETURNS TABLE(multiplier_2 INT, multiplier_4 INT) RETURN SELECT 2, 4")
    sparkSession.sql("SELECT nr * m.multiplier_2, nr * m.multiplier_4 FROM numbers JOIN multipliers() m").show()
  }

}
