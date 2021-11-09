package com.waitingforcode

import org.apache.spark.sql.SparkSession

object StringFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Session window demo").master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    Seq(
      ("2020-01-01"),
      ("xyz"), ("cxyzab"),
      ("2020-01-03"),
      ("abc"), ("cfabccccdefc")
    ).toDF("text").createTempView("test_table")

    val output = sparkSession.sql(
      """
        |SELECT text, REGEXP_LIKE(text, "\\d+-\\d+-\\d+") AS is_date_pattern,
        |BTRIM(text, "cf") AS btrimmed_text,
        |DECODE(text, "xyz", "zyx") AS new_decode_reversed_xyz,
        |ENCODE(text, "UTF-8") AS encoded_text,
        |DECODE(ENCODE(text, "UTF-8") , "UTF-8") AS old_decode
        |FROM test_table
        |""".stripMargin).as[QueryResult].collect().groupBy(queryResult => queryResult.text)
      .map {
        case (key, results) => (key, results.head)
      }

    assert(output("2020-01-01").is_date_pattern)
    assert(!output("abc").is_date_pattern)
    assert(output("abc").btrimmed_text == "ab")
    assert(output("cxyzab").btrimmed_text == "xyzab")
    assert(output("xyz").btrimmed_text == "xyz")
    assert(output("cfabccccdefc").btrimmed_text == "abccccde")
    assert(output("xyz").new_decode_reversed_xyz == "zyx")
    assert(output("cfabccccdefc").new_decode_reversed_xyz == null)
  }

}

case class QueryResult(text: String, is_date_pattern: Boolean, btrimmed_text: String,
                       new_decode_reversed_xyz: String, encoded_text: Array[Byte],
                       old_decode: String)
