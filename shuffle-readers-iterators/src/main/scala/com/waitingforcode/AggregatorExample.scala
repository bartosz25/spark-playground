package com.waitingforcode

import org.apache.spark.sql.{Row, SparkSession}

object AggregatorExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#shuffle-readers #aggregators")
      .master("local[*]")
      //.master("spark://localhost:7077")
      //.config("spark.executor.extraClassPath", sys.props("java.class.path"))
      .config("spark.sql.shuffle.partitions", 10) // reduce tasks
      .config("spark.default.parallelism", 20) // mapper tasks
      .config("spark.reducer.maxSizeInFlight", "2m")
      .config("spark.reducer.maxReqsInFlight", 1)
      .getOrCreate()
    import sparkSession.implicits._
    val usersToGroup = (0 to 3000).map(id => (s"id#${id}", s"login${id % 25}"))
      .toDF("id", "login")

    def mapGroupsTest(key: String, rows: Iterator[Row]): String = {
      s"Number of rows is ${rows.size}"
    }

    val x = usersToGroup.groupByKey(row => row.getAs[String]("login"))
      .mapGroups((key, rows) => {
        s"Number of rows is ${rows.size}"
      })
      //    .count()
      //    .explain(true)
      .collect()
    //println(s"x=${x}")
    usersToGroup.groupByKey(row => row.getAs[String]("login")) .count().explain(true)

    /* this one doesn't require sorting
    show the PairRDD with combiner ==> I should already have an example with the Shuffle writers
    usersToGroup.groupByKey(row => row.getAs[String]("login"))
      .count()
      .show()*/

    //  Thread.sleep(120000)

  }
}
