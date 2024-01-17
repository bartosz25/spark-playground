package com.waitingforcode.table

import org.apache.spark.sql.{DataFrame, SparkSession}

object RateStreamAppWithForeachBatch extends App {

  val sparkSession = SparkSession.builder().master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
    // set a small number for the shuffle partitions to avoid too many debug windows opened
    // (shuffle will happen during the join)
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()
  import sparkSession.implicits._

  val inputDataFrame = sparkSession.readStream.format("rate").option("rowsPerSecond", 10).load()
    .withColumn("number", $"value" % 3)

  inputDataFrame.writeStream.foreachBatch((rateDataFrame: DataFrame, batchNumber: Long) => {
    sparkSession.sql("REFRESH TABLE letters_enrichment_table")
    val referenceDatasetTable = sparkSession.table("letters_enrichment_table")

    val joinedDataset = rateDataFrame.join(referenceDatasetTable, Seq("number"), "inner")
    joinedDataset.show(truncate = false)
    ()
  }).start().awaitTermination()


}
