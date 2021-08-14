package com.waitingforcode.extrafiles.delta

import com.waitingforcode.TestConfiguration
import com.waitingforcode.extrafiles.ItemToWrite
import org.apache.spark.sql.SparkSession

object StaticDataWriterV1 extends App {

  val sparkSession = SparkSession.builder()
    .appName("Delta Lake writer - V1").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val currentTime = System.currentTimeMillis()
  Seq(
    ItemToWrite(0, true, currentTime), ItemToWrite(2, true, currentTime), ItemToWrite(4, true, currentTime),
    ItemToWrite(6, true, currentTime),
  ).toDF.write.format("delta").mode("overwrite").save(TestConfiguration.datasetPath)

}
