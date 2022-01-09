package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object DataDispatcherIfElseStudy {

  def main(args: Array[String]): Unit = {
    val versionToRun = args(0)
    val testSparkSession = SparkSession.builder()
      .appName("TestSparkSession").master("local[*]")
      .getOrCreate()
    val outputDir = "/tmp/spark-if-else"
    val jsonData =
      """{"visit_id": "1", "source": {"site": "localhost"}, "technical": {"browser": {"name": "Firefox", "language": "en"}, "device": {"type": "smartphone"}, "network": "network 1"}}
        |{"visit_id": "2", "source": {"site": "localhost"}, "technical": {"browser": "Firefox", "lang": "en", "device": {"type": "smartphone"},"network": {"short_name": "n1", "long_name": "network 1"}}}
        |{"visit_id": "3", "source": {"site": "www.localhost"}, "technical": {"browser": "Firefox", "lang": "en", "device": {"type": "smartphone"}, "network": "network 1"}}
        |{"visit_id": "4", "source": {"site": "localhost"}, "technical": {"browser": "Firefox", "lang": "en", "device": {"type": {"name": "smartphone"}}, "network": "network 1"}}
        |{"visit_id": "5", "source": {"site": "localhost"}, "technical": {"browser": "Firefox", "lang": "en", "device": {"type": "smartphone"}, "network": "network 1"}}
        |""".stripMargin
    FileUtils.writeStringToFile(new File(s"${outputDir}/data.txt"), jsonData, "utf-8")
    println(s"Running ${versionToRun} version")
    if (versionToRun == "v1") {
      VersionsToRun.runValueOverride(testSparkSession, outputDir)
    } else if (versionToRun == "v2") {
      VersionsToRun.runExternalNestedFields(testSparkSession, outputDir)
    } else if (versionToRun == "v3") {
      VersionsToRun.runOverriddenValueStructColumn(testSparkSession, outputDir)
    } else if (versionToRun == "v4") {
      VersionsToRun.runWithNewColumnStruct(testSparkSession, outputDir)
    }
  }

}
