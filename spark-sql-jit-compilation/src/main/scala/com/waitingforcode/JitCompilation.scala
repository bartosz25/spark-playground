package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator

object JitCompilation {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.codegen.hugeMethodLimit", CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT)
      .getOrCreate()
    import sparkSession.implicits._

    val input = (0 to 100).toDF("nr").repartition(100)
    input.createOrReplaceTempView("input")

    val caseWhens = (0 until 200)
    def generateCaseWhen(expression: String, others: Seq[Int]): String = {
      if (others.isEmpty) {
        expression + " 'big' "
      } else {
        expression + generateCaseWhen("CASE WHEN nr > 0 THEN   ", others.tail) + "  ELSE 'not big' END "
      }
    }
    sparkSession.sql(
      s"""
         |SELECT
         |  ${generateCaseWhen("", caseWhens)} AS x
         |FROM input
         |""".stripMargin).show(false)
  }

}
