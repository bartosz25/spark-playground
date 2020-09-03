package com.waitingforcode.dynamicpartitionpruning

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

trait DatasetsInstaller {
  val warehouseLocation = "/tmp/ddp/data-warehouse"
  val veryBigTable = "very_big_table"
  val smallTable1 = "small_table1"
  val smallTable2 = "small_table2"

  def installDatasets(sparkSession: SparkSession): Unit = {
    val configs = Map(
      veryBigTable -> 5000,
      smallTable1 -> 800,
      smallTable2 -> 20,
    )
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    import sparkSession.implicits._

    FileUtils.deleteDirectory(new File(warehouseLocation))
    configs.foreach {
      case (tableName, maxRows) => {
        sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}").collect()
        def getPartitionNumber(number: Int): Int = {
            if (number < 40) {
              5
            } else if (number < 200) {
              10
            } else {
              20
            }
        }

        val format = "parquet"
        val nr = s"${tableName}_nr"
        val id = s"${tableName}_id"
        val inputDf = (1 to maxRows).map(nr => (nr, getPartitionNumber(nr))).toDF(id, nr)
        inputDf.write.partitionBy(nr).format(format).saveAsTable(tableName)
        sparkSession.sql(s"ANALYZE TABLE ${tableName} COMPUTE STATISTICS FOR COLUMNS ${id}, ${nr}").collect()
      }
    }
  }

}
