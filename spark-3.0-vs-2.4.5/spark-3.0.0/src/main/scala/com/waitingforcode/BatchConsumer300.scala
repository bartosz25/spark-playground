package com.waitingforcode

import org.apache.spark.sql.SparkSession

object BatchConsumer300 extends App {

  case class LetterWithNumber(letter: String, number: Int)

  private val testSparkSession = SparkSession.builder()
    .appName("Spark UI 3.0.0").master("local[*]")
    .config("spark.ui.port", 3000)
    .getOrCreate()
  import testSparkSession.implicits._

  val datasetNumbers = (0 until 1).flatMap(_ => Seq(
    LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3),
    LetterWithNumber("d", 2), LetterWithNumber("e", 1), LetterWithNumber("f", 3), LetterWithNumber("g", 2)
  )).toDS()

  def concatenateLetters(number: Int, letters: Iterator[LetterWithNumber]): String = {
    Thread.sleep(25000L)
    letters.map(_.letter).mkString(",")
  }

  datasetNumbers.as[LetterWithNumber].groupByKey(row => row.number)
    .mapGroups(concatenateLetters)
    .show(truncate=false)

}
