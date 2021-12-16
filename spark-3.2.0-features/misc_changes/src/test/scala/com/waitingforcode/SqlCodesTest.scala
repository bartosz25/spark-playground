package com.waitingforcode

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SqlCodesTest extends AnyFlatSpec with Matchers  {

  it should "return extra fields for Apache Spark 3.2.0" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-34920 : errors with extra fields").master("local[*]")
      .config("spark.sql.ansi.enabled", true) // Otherwise a null is returned instead of the exception
      .getOrCreate()

    val exception = intercept[SparkThrowable] {
      sparkSession.sql("SELECT 3/0").show()
    }
    exception.getSqlState() shouldEqual "22012"
    exception.getErrorClass() shouldEqual "DIVIDE_BY_ZERO"
  }

}
