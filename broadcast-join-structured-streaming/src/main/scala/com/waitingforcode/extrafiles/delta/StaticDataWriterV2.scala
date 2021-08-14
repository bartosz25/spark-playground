package com.waitingforcode.extrafiles.delta

import com.waitingforcode.TestConfiguration
import com.waitingforcode.extrafiles.ItemToWrite
import org.apache.spark.sql.SparkSession

object StaticDataWriterV2 extends App {

  val sparkSession = SparkSession.builder()
    .appName("Delta Lake writer - V2").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val currentTime = System.currentTimeMillis()
  Seq(
    ItemToWrite(10, true, currentTime), ItemToWrite(12, true, currentTime), ItemToWrite(14, true, currentTime),
    ItemToWrite(16, true, currentTime),
  ).toDF.write.format("delta").mode("overwrite").save(TestConfiguration.datasetPath)

}
