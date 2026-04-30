package com.waitingforcode

object ExceptPositionTrapDemo extends App {
  import sparkSession.implicits._

  val df1 = ((1 to 10).map(nr => (s"$nr", s"Number ${nr}")) ++ (1 to 10).map(nr => (s"$nr", s"Number ${nr}"))).toDF("nr", "label")
  val df2 = (3 to 13).map(nr => (s"Number ${nr}", s"$nr")).toDF("label", "nr")

  val missingRowsInDf1 = df1.exceptAll(df2)
  missingRowsInDf1.show(truncate = false)


  val commonRows = df1.intersectAll(df2)
  commonRows.show(truncate = false)
}
