package com.waitingforcode

import org.apache.spark.sql.SparkSession

object PredicatePushdownNestedFieldsDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Nested Fields Predicate Pushdown").master("local[*]")
    .config("spark.sql.parquet.recordLevelFilter.enabled", "true")
    .getOrCreate()
  import sparkSession.implicits._

  val outputDir = "/tmp/wfc/spark3/nested-predicate-pushdown"
  val inputDataset = Seq(
    NestedPredicatePushTestObject(1, NestedPredicatePushEntity("a")),
    NestedPredicatePushTestObject(2, NestedPredicatePushEntity("b")),
    NestedPredicatePushTestObject(3, NestedPredicatePushEntity("a")),
    NestedPredicatePushTestObject(4, NestedPredicatePushEntity("c")),
    NestedPredicatePushTestObject(5, NestedPredicatePushEntity("a"))
  ).toDS()
  inputDataset.write.mode("overwrite").parquet(outputDir)


  sparkSession.read
    .parquet(outputDir)
    .filter("nestedField.letter = 'a'")
    .show(20)


}

case class NestedPredicatePushTestObject(id: Int, nestedField: NestedPredicatePushEntity)
case class NestedPredicatePushEntity(letter: String)
