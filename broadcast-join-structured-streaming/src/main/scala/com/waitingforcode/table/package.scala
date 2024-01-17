package com.waitingforcode

package object table {

  val outputDir = "/tmp/join_structured_streaming/table"
  val dataWarehouseBaseDir = s"${outputDir}/warehouse"
  System.setProperty("derby.system.home", dataWarehouseBaseDir)
}
