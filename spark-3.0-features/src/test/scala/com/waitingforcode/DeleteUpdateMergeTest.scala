package com.waitingforcode

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeleteUpdateMergeTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: delete/update/merge").master("local[*]").enableHiveSupport()
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate()
  import sparkSession.implicits._

  before {
    sparkSession.sql("DROP TABLE IF EXISTS numbers_delete")
  }

  "the DELETE operation" should "be supported if doesn't contain subquery" in {
    sparkSession.sql("CREATE TABLE numbers_delete (id bigint, number int) USING HIVE PARTITIONED BY (id)")
    sparkSession.sql("INSERT INTO numbers_delete VALUES (1, 1), (2, 2), (3, 3)")

    val analysisException = intercept[AnalysisException] {
      sparkSession.sql("DELETE FROM numbers_delete WHERE number = 3")
    }

    analysisException.getMessage() should include("DELETE is only supported with v2 tables")
  }


  "the UPDATE operation" should "not be supported" in {
    val numbers = Seq((1), (2), (3)).toDF("number")
    numbers.createOrReplaceTempView("numbers_update")

    val updateError = intercept[UnsupportedOperationException] {
      sparkSession.sql("UPDATE numbers_update SET number = 3 WHERE number = 2")
    }

    updateError.getMessage should include("UPDATE TABLE is not supported temporarily")
  }

  "the MERGE operation" should "not be supported" in {
    val numbers = Seq((1), (2), (3)).toDF("number")
    numbers.createOrReplaceTempView("numbers_merge_source")
    numbers .createOrReplaceTempView("numbers_merge_target")

    val mergeError = intercept[UnsupportedOperationException] {
      sparkSession.sql(
        """
          |MERGE INTO numbers_merge_target AS target
          | USING numbers_merge_source AS source ON target.number = source.number
          |WHEN MATCHED THEN UPDATE SET target.number = source.number
          |""".stripMargin)
    }

    mergeError.getMessage should include("MERGE INTO TABLE is not supported temporarily")
  }


}
