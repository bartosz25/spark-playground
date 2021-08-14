package com.waitingforcode

import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.sql.SparkSession

object StageLevelSchedulingApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#kubernetes-demo")
      .getOrCreate()
    val inputRddNumbers = sparkSession.sparkContext.parallelize(0 to 10)

    val resourceProfileBuilder = new ResourceProfileBuilder()
    resourceProfileBuilder.require(new ExecutorResourceRequests().cores(2).offHeapMemory("50MB"))
    resourceProfileBuilder.require(new TaskResourceRequests().cpus(1))
    inputRddNumbers.withResources(resourceProfileBuilder.build())

    // Let's introduce a new stage here
    val evenOddGroups = inputRddNumbers.groupBy(nr => nr % 2 == 0)
    evenOddGroups.withResources(new ResourceProfileBuilder()
      .require(new ExecutorResourceRequests().cores(4).memory("1200MB")).build())
    evenOddGroups.count()
    sparkSession.sparkContext.stop()
  }

}
