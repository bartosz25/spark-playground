package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

object QueryExecutionListenerExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    sparkSession.listenerManager.register(QueryExecutionPrintingListener)

    import sparkSession.implicits._
    (0 to 100).toDF("nr").count()
  }

}

object QueryExecutionPrintingListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println("=========================================")
    println("                 Print on success                 ")
    println(s"${funcName}: ${qe}")
    println("=========================================")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println("=========================================")
    println("                 Print on failure                 ")
    println(s"${funcName}: Failure!")
    println("=========================================")
  }
}
