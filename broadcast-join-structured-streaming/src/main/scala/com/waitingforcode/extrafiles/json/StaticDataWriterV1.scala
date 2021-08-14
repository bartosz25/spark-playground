package com.waitingforcode.extrafiles.json

import com.waitingforcode.TestConfiguration
import org.apache.commons.io.FileUtils

import java.io.File

object StaticDataWriterV1 extends App {

  val filesToWrite = Seq("file1.json", "file2.json")

  val dataToWritePerFile = Map(
    "file1.json" -> s"""
                       |{"nr": 0, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |{"nr": 2, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |""".stripMargin,
    "file2.json" -> s"""
                       |{"nr": 4, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |{"nr": 6, "is_even": true, "ts": "${System.currentTimeMillis()}"}
                       |""".stripMargin
  )

  FileUtils.deleteDirectory(new File(TestConfiguration.datasetPath))
  filesToWrite.foreach(file => {
    FileUtils.writeStringToFile(new File(s"${TestConfiguration.datasetPath}/${file}"),
      dataToWritePerFile(file), "UTF-8")
  })
}
