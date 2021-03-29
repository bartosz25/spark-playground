package com.waitingforcode

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

object PredicatePushdownJson extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("#json-predicate-pushdown").master("local[*]")
    .config("spark.default.parallelism", 1)
    .getOrCreate()
  import testSparkSession.implicits._

  val numbers = (0 to 5).map(nr => NestedNumber(nr, nr, NestedNumberChild(nr + 1, nr + 2)))
    .toDF()
  val dataPath = "/tmp/wfc/spark-3.1.1/json-predicate-pushdown"
  numbers.write.mode(SaveMode.Overwrite).json(dataPath)

  val filteredRows = testSparkSession.read
    .schema(ScalaReflection.schemaFor[NestedNumber].dataType.asInstanceOf[StructType])
    .json(dataPath)
    .filter("topLevelNumber2 > 3 OR topLevelNumber1 > 3") // predicate pushdown expected
    .filter("nestedNumber.topLevelNumber2 > 1") // nested fields; predicate pushdown doesn't expected

  // Show first how to distinguish a plan with pushed predicates
  //filteredRows.explain(true)

  filteredRows.show()

}

case class NestedNumber(topLevelNumber1: Int, topLevelNumber2: Int, nestedNumber: NestedNumberChild)
case class NestedNumberChild(topLevelNumber1: Int, topLevelNumber2: Int)