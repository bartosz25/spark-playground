package com.waitingforcode.state_evolution_problem

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.{SaveMode, functions}

import java.sql.Timestamp
import java.time.Instant
import java.util.{TimeZone, UUID}
import scala.language.implicitConversions

case class Letter(id: Int, lowerCase: String)

object DataGenerator {

  val OutputPath = "/tmp/wfc/state_evolution/input"

  def generate(saveMode: SaveMode): Unit = {
    val sparkSession = SparkSession.active
    import sparkSession.implicits._

    val dfToWrite = Seq(
      Letter(id = 1, lowerCase = "a"),
      Letter(id = 2, lowerCase = "a")
    ).toDF()

    dfToWrite.repartition(1).write.mode(saveMode).format("json").save(OutputPath)
  }

}