package com.waitingforcode.solutions

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object DeltaLakeReplaceWhereSaveAsTableExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/partitions/delta-lake-replacewhere-saveastable-example"
    FileUtils.deleteDirectory(new File(outputDir))
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[2]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    Seq((3, "C", "c"), (4, "D", "d")).toDF("nr", "upper_case", "lower_case").write.format("delta")
      .mode(SaveMode.Overwrite).partitionBy("nr")
      .saveAsTable("test_table")
    sparkSession.sql("SELECT * FROM test_table").show()

    Seq(("Cc", 3, "cc")).toDF("upper_case", "nr", "lower_case").write.format("delta")
      .option("replaceWhere", "nr = 3")
      .mode(SaveMode.Overwrite)
      .saveAsTable("test_table")
    sparkSession.sql("SELECT * FROM test_table").show()

    //while (true) {}
  }

}