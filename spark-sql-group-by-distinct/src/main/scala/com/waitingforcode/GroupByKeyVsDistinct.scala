package com.waitingforcode

import org.apache.spark.sql.SparkSession

object GroupByKeyVsDistinct {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#group-by-distinct")
      .master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    (0 to 10).map(id => (s"id#${id}", s"login${id}"))
      .toDF("id", "login").createTempView("users")

    // COUNT vs COUNT(DISTINCT)
    sparkSession.sql("SELECT COUNT(COUNT(login)) FROM users GROUP BY login").explain(true)
    sparkSession.sql("SELECT COUNT(DISTINCT(login)) FROM users").explain(true)

    // Drop duplicates
    sparkSession.sql("SELECT login FROM users").dropDuplicates("login").explain(true)

    // self-join with DISTINCT vs GROUP BY
    (0 to 30).map(nr => ("article#1", s"login${nr % 10}")).toDF("article", "user_login")
      .createTempView("liked_articles")
    Seq((20, 20), (50, 25), (70, 90), (90, 70), (90, 70)).toDF("v1", "v2").createTempView("scores")
    sparkSession.sql("SELECT DISTINCT s1.* FROM scores s1, scores s2 " +
      "WHERE s1.v1 = s2.v2 AND s1.v2 = s2.v1 AND s1.v1 <= s1.v2").explain(true)
    sparkSession.sql("SELECT s1.* FROM scores s1, scores s2 " +
          "WHERE s1.v1 = s2.v2 AND s1.v2 = s2.v1 AND s1.v1 <= s1.v2 GROUP BY s1.v1, s1.v2").explain(true)

  }
}