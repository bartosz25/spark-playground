package com.waitingforcode

import org.apache.spark.sql.SparkSession

object MiscFunctions {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark 3.3.0. SQL functions").master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      """
        |SELECT
        | AES_ENCRYPT('a b c d e', 'passphrase_12345') AS encrypted,
        | CAST(AES_DECRYPT(AES_ENCRYPT('a b c d e', 'passphrase_12345'), 'passphrase_12345') AS STRING) AS decrypted
        |""".stripMargin).show(true)
    // Prints
    /*
    +--------------------+---------+
    |           encrypted|decrypted|
    +--------------------+---------+
    |[39 DC 4C A5 08 6...|a b c d e|
    +--------------------+---------+
     */

    sparkSession.sql(
      """
        |SELECT
        | CEIL(3.53221, 3),
        | FLOOR(3.53221, 3)
        |""".stripMargin).show(false)
    // Prints
    /*
    +----------------+-----------------+
    |ceil(3.53221, 3)|floor(3.53221, 3)|
    +----------------+-----------------+
    |3.533           |3.532            |
    +----------------+-----------------+
     */

  }

}
