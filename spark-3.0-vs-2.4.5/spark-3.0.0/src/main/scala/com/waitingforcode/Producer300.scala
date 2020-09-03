package com.waitingforcode

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.sql.SparkSession



// It's here only to produce some data, so can be shared between 2 versions
object Producer300 extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("Data Producer").master("local[1]")
    .config("spark.ui.port", 4041)
    .getOrCreate()
  import testSparkSession.implicits._

  (0 to 1000).foreach(batch => {
    val numbersToWrite = ThreadLocalRandom.current().ints(5)
      .toArray
      .map(nr => (nr.toString)).toSeq.toDF("value")

    numbersToWrite.write.format("kafka").option("kafka.bootstrap.servers", "localhost:29092")
      .option("topic", "test_topic")
      .save()

    Thread.sleep(3000)
  })

}
