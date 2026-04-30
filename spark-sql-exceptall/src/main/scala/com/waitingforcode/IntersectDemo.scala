package com.waitingforcode

object IntersectDemo extends App {

  import sparkSession.implicits._

  val df1 = ((1 to 10).map(nr => (nr, s"Number ${nr}")) ++ (1 to 10).map(nr => (nr, s"Number ${nr}"))).toDF("nr", "label")
  val df2 = ((3 to 13).map(nr => (nr, s"Number ${nr}")) ++ (3 to 5).map(nr => (nr, s"Number ${nr}"))).toDF("nr", "label")

  val commonRowsNoDuplicates = df1.intersect(df2)
  commonRowsNoDuplicates.explain(extended = true)
  commonRowsNoDuplicates.show(truncate = false)
  /*
  both DFs have this in common:
  +---+---------+
  |nr |label    |
  +---+---------+
  |3  |Number 3 |
  |4  |Number 4 |
  |5  |Number 5 |
  |6  |Number 6 |
  |7  |Number 7 |
  |8  |Number 8 |
  |9  |Number 9 |
  |10 |Number 10|
  +---+---------+
   */

  val commonRowsWithDuplicates = df1.intersectAll(df2)
  commonRowsWithDuplicates.explain(extended = true)
  commonRowsWithDuplicates.show()
  /*
  This is really looking for pairs; duplicated rows in df2 are included but duplicated rows in df1 are not
  +---+---------+
  | nr|    label|
  +---+---------+
  |  3| Number 3|
  |  3| Number 3|
  |  4| Number 4|
  |  4| Number 4|
  |  5| Number 5|
  |  5| Number 5|
  |  6| Number 6|
  |  7| Number 7|
  |  8| Number 8|
  |  9| Number 9|
  | 10|Number 10|
  +---+---------+
   */
}
