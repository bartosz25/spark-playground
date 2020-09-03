package com.waitingforcode.dynamicpartitionpruning

import org.apache.spark.sql.SparkSession

object DynamicPartitionPruningEnabledDisabledDemo extends App with DatasetsInstaller {

  val sparkSession = SparkSession.builder().master("local[*]")
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", true)
    .config("spark.sql.adaptive.enabled", false)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .getOrCreate()
  installDatasets(sparkSession)

  val ddpQueryDifferentNames =  s"""
       |SELECT vb.${veryBigTable}_id, s.${smallTable1}_nr FROM ${veryBigTable} vb
       |JOIN ${smallTable1} s ON vb.${veryBigTable}_nr = s.${smallTable1}_nr
       |WHERE s.${smallTable1}_id = 5
       |""".stripMargin

  val df = sparkSession.sql(ddpQueryDifferentNames)
  df.explain(true)

}
