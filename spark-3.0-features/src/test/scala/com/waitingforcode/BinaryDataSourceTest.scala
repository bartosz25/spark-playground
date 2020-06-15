package com.waitingforcode

import java.io.File
import java.sql.Timestamp

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BinaryDataSourceTest extends AnyFlatSpec with Matchers {

  private val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: binary data source").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  "an Apache Spark logo" should "be correctly read with a binary data source" in {
    val path = getClass.getResource("/binary_files").getPath
    val sparkLogo = sparkSession.read.format("binaryFile")
        .load(path).as[SparkLogoFields]

    val files = sparkLogo.collect()
    files should have size 1
    val logo = files(0)
    logo.path should endWith("binary_files/apache_spark_logo.png")
    logo.modificationTime shouldNot be(null)
    logo.length shouldEqual 67362
    logo.content shouldEqual FileUtils.readFileToByteArray(new File(s"${path}/apache_spark_logo.png"))
  }

}
case class SparkLogoFields(path: String, modificationTime: Timestamp, length: BigInt, content: Array[Byte])
