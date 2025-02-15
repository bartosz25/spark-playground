package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object SaveAsTableDynamicPartitionOverwriteExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/partitions/saveastable-dynamic-overwrite-example"
    FileUtils.deleteDirectory(new File(outputDir))
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[2]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    Seq((3, "C", "c"), (4, "D", "d")).toDF("nr", "upper_case", "lower_case").write
      .mode(SaveMode.Overwrite).partitionBy("nr")
      .saveAsTable("test_table")
    sparkSession.sql("SELECT * FROM test_table").show()

    Seq(("Cc", "cc", 3), ("E", "e", 5)).toDF("upper_case", "lower_case", "nr").write
      .partitionBy("nr")
      .mode(SaveMode.Overwrite)
      .saveAsTable("test_table")
    sparkSession.sql("SELECT * FROM test_table").show()
    sparkSession.sql("SHOW PARTITIONS test_table").show()
    //while (true) {}
  }

}