package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

object FilterAccumulatorWithTemporaryErrorDemo2 {

  def main(args: Array[String]): Unit = {
    val partitions = 5
    val sparkSession = SparkSession.builder().master(s"local[${partitions}, 3]")
      .config("spark.task.maxFailures", 3)
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = (0 to 100).map(nr => UserToTest(nr, s"user${nr}")).toDS.repartition(partitions)

    val idFilterAccumulator = sparkSession.sparkContext.longAccumulator("idFilterAccumulator")
    val evenIdFilterAccumulator = sparkSession.sparkContext.longAccumulator("lowerUpperCaseFilterAccumulator")
    val idFilter = new FilterWithAccumulatedResultWithFailure(
      (user) => user.id > 0, idFilterAccumulator
    )
    val evenIdFilter = new FilterWithAccumulatedResultWithFailure(
      (user) => user.id % 2 == 0, evenIdFilterAccumulator
    )

    Try {
      val filteredInput = dataset.filter(idFilter.filter _).filter(evenIdFilter.filter _)
      filteredInput.show(false)
    }
    println(s"idFilterAccumulator=${idFilterAccumulator.count}")
    println(s"evenIdFilterAccumulator=${evenIdFilterAccumulator.count}")
  }

}


class FilterWithAccumulatedResultWithFailure(filterMethod: (UserToTest) => Boolean, resultAccumulator: LongAccumulator) extends Serializable {

  def filter(userToTest: UserToTest): Boolean = {
    val result = filterMethod(userToTest)
    if (!result) resultAccumulator.add(1L)
    if (!result && userToTest.id == 11 && FailureFlagHolder.isFailed.incrementAndGet() < 3) {
      throw new RuntimeException("temporary error")
    }
    result
  }

}

object FailureFlagHolder {

  val isFailed = new AtomicInteger(0)

}