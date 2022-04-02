package com

package object waitingforcode {

  val dataWarehouseBaseDir = "/tmp/spark-tables/"
  System.setProperty("derby.system.home", dataWarehouseBaseDir)

  val dataWarehouseInternalTablesDirectory = s"${dataWarehouseBaseDir}/internal-table"
  val dataWarehouseExternalTablesDirectory = s"${dataWarehouseBaseDir}/external-table"

  val externalDataOutputDir = "/tmp/spark-external-tables-data"
}
