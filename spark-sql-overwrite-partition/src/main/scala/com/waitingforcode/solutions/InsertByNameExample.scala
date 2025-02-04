package com.waitingforcode.solutions

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Union}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.io.File

object InsertByNameExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/spark-playground/partitions/insertbyname-example"
    FileUtils.deleteDirectory(new File(outputDir))
    val dataWarehouseBaseDir = s"${outputDir}/warehouse"
    System.setProperty("derby.system.home", dataWarehouseBaseDir)
    val sparkSession = SparkSession.builder().master(s"local[2]")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    Seq((3, "C", "c"), (4, "D", "d")).toDF("nr", "upper_case", "lower_case").write
      .mode(SaveMode.Overwrite).partitionBy("nr")
      .saveAsTable("test_table")
    sparkSession.sql("SELECT * FROM test_table").show()

    println(sparkSession.table("test_table").columns.mkString(","))
    println(Seq(("Cc", 3, "cc")).toDF("upper_case", "nr", "lower_case").columns.mkString(","))
    insertByName("test_table", Seq(("Cc", 3, "cc")).toDF("upper_case", "nr", "lower_case"))
    sparkSession.sql("SELECT * FROM test_table").show()

    //while (true) {}
  }

  private def insertByName(tableName: String, dataToInsert: DataFrame): Unit = {
    val tableSchema = dataToInsert.sparkSession.table(tableName).schema
    val dataset = dataToInsert.sparkSession.createDataFrame(new java.util.ArrayList[Row], tableSchema)
    //dataset.unionByName(dataToInsert).explain(true)
    dataset.unionByName(dataToInsert).write.mode(SaveMode.Overwrite).insertInto(tableName)
  }

}