package com.waitingforcode

import org.apache.spark.sql.SparkSession

object StaticApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#kubernetes-demo")
      .getOrCreate()
    import sparkSession.implicits._
    (0 to 10).toDF().count()

    //Thread.sleep(960000)
    sparkSession.sparkContext.stop()
  }

}
