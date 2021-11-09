package com.waitingforcode

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{Row, SparkSession}

object DefaultStateArbitraryStatefulProcessingDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Kafka changes demo").master("local[*]")
    .config("spark.sql.shuffle.partitions", 2) // 2 physical state stores; one per shuffle partition task
    .getOrCreate()

  import sparkSession.implicits._

  val defaultState = Seq(
    ("user1", 10), ("user2", 20), ("user3", 30)
  ).toDF("login", "points").as[(String, Int)].groupByKey(row => row._1).mapValues(_._2)

  val inputStream = new MemoryStream[(String, Int)](1, sparkSession.sqlContext)
  inputStream.addData(("user1", 5))
  inputStream.addData(("user4", 2))

  val statefulAggregation = inputStream.toDS().toDF("login", "points")
    .groupByKey(row => row.getAs[String]("login"))
    .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout(), defaultState)(StatefulMapper.apply)

  val writeQuery = statefulAggregation.writeStream.outputMode("update").format("console")

  writeQuery.start().awaitTermination()

}

object StatefulMapper {

  def apply(key: String, logs: Iterator[Row], state: GroupState[Int]): Int = {
    println(s"Got state=${state} for key=${key}")
    println(s"State is ${state.hasTimedOut}")
    val newPoints = logs.map(log => log.getAs[Int]("points")).sum + state.getOption.getOrElse(0)
    state.update(newPoints)
    newPoints
  }

}