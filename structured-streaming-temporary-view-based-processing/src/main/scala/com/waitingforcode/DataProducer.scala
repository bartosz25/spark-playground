package com.waitingforcode


import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.sql.SparkSession

object DataProducer extends App {

  private val testSparkSession = SparkSession.builder()
    .appName("Data Producer").master("local[*]")
    .getOrCreate()
  import testSparkSession.implicits._

  (0 to 1000).foreach(batch => {
    val numbersToWrite = ThreadLocalRandom.current().ints(5)
      .toArray
      .map(nr => (nr.toString)).toSeq.toDF("value")

    numbersToWrite.write.format("kafka").option("kafka.bootstrap.servers", "localhost:29092")
      .option("topic", SyntaxCheckConfiguration.TopicName)
      .save()

    Thread.sleep(3000)
  })

}

object SyntaxCheckConfiguration {
  val TopicName = "syntax_check"
}
