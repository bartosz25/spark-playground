package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object FilterAccumulatorDemo1 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2, 2]")
      .config("spark.task.maxFailures", 2)
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = (0 to 100).map(nr => UserToTest(nr, s"user${nr}")).toDS

    val idFilterAccumulator = sparkSession.sparkContext.longAccumulator("idFilterAccumulator")
    val evenIdFilterAccumulator = sparkSession.sparkContext.longAccumulator("lowerUpperCaseFilterAccumulator")
    val idFilter = new FilterWithAccumulatedResult(
      (user) => user.id > 0, idFilterAccumulator
    )
    val evenIdFilter = new FilterWithAccumulatedResult(
      (user) => user.id % 2 == 0, evenIdFilterAccumulator
    )


    val filteredInput = dataset.filter(idFilter.filter _).filter(evenIdFilter.filter _)
    filteredInput.collect()
    println(s"idFilterAccumulator=${idFilterAccumulator.count}")
    println(s"evenIdFilterAccumulator=${evenIdFilterAccumulator.count}")
  }

}

class FilterWithAccumulatedResult(filterMethod: (UserToTest) => Boolean, resultAccumulator: LongAccumulator) extends Serializable {

  def filter(userToTest: UserToTest): Boolean = {
    val result = filterMethod(userToTest)
    if (!result) resultAccumulator.add(1L)
    result
  }

}