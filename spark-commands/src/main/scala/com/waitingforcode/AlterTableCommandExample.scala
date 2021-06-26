package com.waitingforcode

import org.apache.spark.sql.SparkSession

object AlterTableCommandExample extends App {

  val sparkSession = SparkSession.builder()
    .appName("Alter table example").master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  // How many of these are commands? All except the saveAsTable and outputShowTables.write!

  // DropTableCommand
  sparkSession.sql("DROP TABLE users_list")
  import sparkSession.implicits._
  val usersToGroup = (0 to 10).map(id => (s"id#${id}", s"login${id}"))
    .toDF("id", "login")
  usersToGroup.write.saveAsTable("users_list")

  // AlterTableAddColumnsCommand#1
  val outputAlterTable = sparkSession.sql("ALTER TABLE users_list ADD COLUMNS first_name STRING")
  outputAlterTable.show(false)
  outputAlterTable.show(false)
  outputAlterTable.show(false)
  outputAlterTable.show(false)
  outputAlterTable.show(false)

  // AlterTableAddColumnsCommand#1
  // The lazy side effects field would work for the sql(...) + show(...) but
  // not for rerunning the same command!
  // Uncomment and rerun to see this!
  sparkSession.sql("ALTER TABLE users_list ADD COLUMNS first_name STRING")

  // ShowTablesCommand
  // Example to show the output generation, missing in the ALTER or DROP
  val outputShowTables = sparkSession.sql("SHOW TABLES")
  // Prove that SHOW TABLES is not called 3 times here!
  outputShowTables.show(false)
  outputShowTables.write.csv(s"/tmp/test${System.currentTimeMillis()}")

  Thread.sleep(200000)
}
