package com.waitingforcode

import org.apache.spark.sql.SparkSession

object GroupByKeyVsDistinct {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#shuffle-readers #aggregators")
      .master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
/*
    val data = Seq(
      "{'number': 1}",
      "{'number': }"
    )

    val schema = new StructType()
      .add($"number".int)
      .add($"_corrupt_record".string)

    val sourceDf = data.toDF("column")

    val jsonedDf = sourceDf
      .select(from_json(
        $"column",
        schema,
        Map("mode" -> "FAILFAST", "columnNameOfCorruptRecord" ->
          "_corrupt_record")
      ) as "data").selectExpr("data.number", "data._corrupt_record")

    jsonedDf.show()*/

    var df = (0 to 10).map(id => (s"id#${id}", s"login${id}"))
      .toDF("id", "login")
    for (i <- 0 to 3) {
      df = df.union(df)
    }
    df.show(50,true)



    (0 to 10).map(id => (s"id#${id}", s"login${id}"))
      .toDF("id", "login").createTempView("users")

    (0 to 10).map(id => (s"id#${id}", s"login${id}"))
      .toDF("id", "login").distinct().explain(true)
    (0 to 10).map(id => (s"id#${id}", s"login${id}"))
      .toDF("id", "login").dropDuplicates().explain(true)

    (0 to 10).map(id => (s"id#${id}", s"login${id}"))
      .toDF("id", "login").groupBy($"id")
    //sparkSession.sql("SELECT COUNT(COUNT(login)) FROM users GROUP BY login").explain(true)
    //sparkSession.sql("SELECT COUNT(DISTINCT(login)) FROM users").explain(true)
    //sparkSession.sql("SELECT login FROM users").dropDuplicates("login").explain(true)
    /*
    (0 to 30).map(nr => ("article#1", s"login${nr % 10}")).toDF("article", "user_login")
      .createTempView("liked_articles")

    Seq((20, 20), (50, 25), (70, 90), (90, 70), (90, 70)).toDF("v1", "v2").createTempView("scores")
    sparkSession.sql("SELECT DISTINCT s1.* FROM scores s1, scores s2 " +
      "WHERE s1.v1 = s2.v2 AND s1.v2 = s2.v1 AND s1.v1 <= s1.v2").explain(true)

    sparkSession.sql("SELECT s1.* FROM scores s1, scores s2 " +
          "WHERE s1.v1 = s2.v2 AND s1.v2 = s2.v1 AND s1.v1 <= s1.v2 GROUP BY s1.v1, s1.v2").explain(true)
*/
  }
}
