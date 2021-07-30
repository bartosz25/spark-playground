package com.waitingforcode

import org.apache.spark.sql.SparkSession

object NotInVsNotExistsSingleColumn {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("#not-in-vs-not-exists")
      .master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    ((0 to 10).map(id => User(Some(s"id#${id}"), s"login${id}", Some(id))) ++
      Seq(User(None, "null login", None)))
      .toDF.createTempView("users")
    ((4 to 10).map(id => RegisteredUser(Some(s"id#${id}"), 1000, Some(id))) ++
      Seq(RegisteredUser(Some("id#34"), 1200, Some(4)),
        RegisteredUser(None, 1300, None)
      ))
      .toDF.createTempView("registered")

    val notInQuery = sparkSession.sql(
      """
        |SELECT id, login, number FROM users WHERE id NOT IN (SELECT userId FROM registered)
        |""".stripMargin)
    notInQuery.explain(true)
    //notInQuery.show(true)

    val notExistsQuery = sparkSession.sql(
      """
        |SELECT id, login, number FROM users WHERE NOT EXISTS (SELECT 1 FROM registered r
        |WHERE r.userId = id)
        |""".stripMargin)
    notExistsQuery.explain(true)
    //notExistsQuery.show(false)

    Thread.sleep(960000)
  }
}