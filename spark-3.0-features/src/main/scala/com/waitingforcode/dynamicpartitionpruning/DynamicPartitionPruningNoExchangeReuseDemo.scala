package com.waitingforcode.dynamicpartitionpruning

import org.apache.spark.sql.SparkSession


object DynamicPartitionPruningNoExchangeReuseDemo extends App with DatasetsInstaller {

  val sparkSession = SparkSession.builder().master("local[*]")
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", true)
    .config("spark.sql.adaptive.enabled", false)
    .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", false)
    .config("spark.sql.exchange.reuse", false)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .getOrCreate()

  installDatasets(sparkSession)

  val ddpQueryDifferentNames =  s"""
                                   |SELECT vb.${veryBigTable}_id, s.${smallTable1}_nr FROM ${smallTable1} s
                                   |JOIN ${veryBigTable} vb ON vb.${veryBigTable}_nr = s.${smallTable1}_nr
                                   |WHERE s.${smallTable1}_id = 5
                                   |""".stripMargin

  val df = sparkSession.sql(ddpQueryDifferentNames)
  df.explain(true)
}
