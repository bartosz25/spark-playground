package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

object AcidGuaranteeExample extends App {

  val dataWorkingDir = "/tmp/delta-acid/"
  val jsonDir = s"${dataWorkingDir}/json"
  val deltaDir = s"${dataWorkingDir}/delta"
  FileUtils.deleteDirectory(new File(dataWorkingDir))

  val localSparkSession: SparkSession = SparkSession.builder()
    .appName("[Delta Lake] ACID guarantee").master("local[*]").getOrCreate()
  import localSparkSession.implicits._

  val numbers = (0 to 10).toDF("value").repartition(5)

  // Let's start by writing a JSON file with Spark, so no ACID support
  println("[DEBUG] Let's generate the JSON data without a failure")
  numbers.write.mode("overwrite").json(jsonDir)
  localSparkSession.read.json(jsonDir).show(false)
  try {
    numbers.as[Int] // Add a breakpoint here, when reached do `ls /tmp/nad/data-acid/`
      .map(number => {
        if (number == 8) {
          throw new RuntimeException("Planed exception, broken consistency!")
        }
        number
      }).write.mode("overwrite").json(jsonDir)
  } catch {
    case NonFatal(e) => print("We don't care about this, it's a planned exception")
  }
  println("[DEBUG] The dir should be empty! Use the text data source to avoid schema inference problems")
  localSparkSession.read.text(jsonDir).show(false)

  println("[DEBUG] Let's do now the same exercise but for Delta. First, we'll have a write that succeeds")
  numbers.write.format("delta").save(deltaDir)
  localSparkSession.read.format("delta").load(deltaDir).show(truncate = false)

  println("[DEBUG] Let's try again to write the data, but this time it will fail")
  try {
    val numbersFrom20 = (20 to 30).toDF("value")
    numbersFrom20.as[Int]
      .map(number => {
        if (number == 22) {
          throw new RuntimeException("Planed exception, broken consistency!")
        }
        number
      }).write.format("delta").mode("overwrite").save(deltaDir)
  } catch {
    case NonFatal(e) => print("We don't care about this, it's a planned exception")
  }
  println("[DEBUG] Despite the failure, we should still have only 10 most recent rows")
  localSparkSession.read.format("delta").load(deltaDir).show(truncate = false)

}
