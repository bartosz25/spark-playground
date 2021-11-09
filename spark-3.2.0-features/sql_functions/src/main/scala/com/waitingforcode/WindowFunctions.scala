package com.waitingforcode

import org.apache.spark.sql.SparkSession

object WindowFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Session window demo").master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    Seq(
      ("Team A", "Player 1", Some(3)),
      ("Team A", "Player 2", None),
      ("Team A", "Player 3", Some(6)),
      ("Team B", "Player 1", None),
      ("Team B", "Player 2", Some(12)),
    ).toDF("team", "player", "points").createTempView("test_table")

    val output = sparkSession.sql(
      """
        |SELECT team, player, points,
        |nth_value(points, 1) IGNORE NULLS OVER team_players_window AS nth_value_1_ignored_nulls,
        |nth_value(points, 1) OVER team_players_window AS nth_value_1_not_ignored_nulls,
        |LAG(points) IGNORE NULLS OVER team_players_window AS lag_ignored_nulls,
        |LAG(points) OVER team_players_window AS lag_not_ignored_nulls
        |FROM test_table
        |WINDOW team_players_window AS (PARTITION BY team ORDER BY player)
        |""".stripMargin).as[WindowQueryResult].collect()
    .groupBy(result => s"${result.team}-${result.player}").map {
      case (groupKey, values) => (groupKey, values.head)
    }

    assert(output("Team A-Player 3").lag_ignored_nulls == Some(3))
    assert(output("Team A-Player 3").lag_not_ignored_nulls == None)
    assert(output("Team B-Player 2").nth_value_1_ignored_nulls == Some(12))
    assert(output("Team B-Player 2").nth_value_1_not_ignored_nulls == None)
  }

}

case class WindowQueryResult(team: String, player: String, points: Option[Int],
                             nth_value_1_ignored_nulls: Option[Int], nth_value_1_not_ignored_nulls: Option[Int],
                             lag_ignored_nulls: Option[Int], lag_not_ignored_nulls: Option[Int])
