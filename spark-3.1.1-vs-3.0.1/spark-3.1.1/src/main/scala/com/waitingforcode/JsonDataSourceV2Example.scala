package com.waitingforcode

import org.apache.spark.sql.{SaveMode, SparkSession}


object JsonDataSourceV2Example extends App {

  val sparkSession = SparkSession.builder()
    .appName("#JSON-DataSource-V2").master("local[*]")
    .config("spark.sql.sources.useV1SourceList", "")
    .getOrCreate()
  import sparkSession.implicits._

  val usersFromShop = (0 to 10).map(nr => (s"User#${nr}", nr)).toDF("login", "id")
  usersFromShop.write.mode(SaveMode.Overwrite).json("/tmp/test-datasource-v2")

  sparkSession.read.schema(usersFromShop.schema).json("/tmp/test-datasource-v2").show(false)
}
