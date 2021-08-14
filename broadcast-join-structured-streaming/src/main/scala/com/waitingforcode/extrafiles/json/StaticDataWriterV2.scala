package com.waitingforcode.extrafiles.json

import com.waitingforcode.TestConfiguration
import org.apache.commons.io.FileUtils

import java.io.File

object StaticDataWriterV2 extends App {
  // Version with 3 files - file3.json will contain matching rows!
  val filesToWrite = Seq("file1.json", "file2.json", "file3.json")

  val dataToWritePerFile = Map(
    "file1.json" -> "",
    "file2.json" -> "",
    "file3.json" -> s"""
                       |{"nr": 10, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |{"nr": 12, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |{"nr": 14, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |{"nr": 16, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |""".stripMargin
  )
  FileUtils.deleteDirectory(new File(TestConfiguration.datasetPath))
  filesToWrite.foreach(file => {
    FileUtils.writeStringToFile(new File(s"${TestConfiguration.datasetPath}/${file}"),
      dataToWritePerFile(file), "UTF-8")
  })
  FileUtils.writeStringToFile(new File(s"${TestConfiguration.datasetPath}/_SUCCESS"),
    "", "UTF-8")
}
