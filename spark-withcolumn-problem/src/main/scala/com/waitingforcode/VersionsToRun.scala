package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}

object VersionsToRun {

  def runValueOverride(sparkSession: SparkSession, inputDir: String): Unit = {
    import sparkSession.implicits._
    sparkSession.read.text(inputDir)
      .select(functions.from_json($"value", EventLog.Schema).as("value"))
      .withColumn("value", CleanserFunctionsStudy.anonymizeUser())
      .withColumn("value", CleanserFunctionsStudy.cleanseBrowser())
      .withColumn("value", CleanserFunctionsStudy.cleanseDevice())
      .withColumn("value", CleanserFunctionsStudy. cleanseNetwork())
      .withColumn("value", CleanserFunctionsStudy.cleanseSource())
      .withColumn("value", functions.to_json($"value")).select("value")
      .write.mode("overwrite").text("/tmp/some_output")
  }

  def runExternalNestedFields(sparkSession: SparkSession, inputDir: String): Unit = {
    import sparkSession.implicits._
    sparkSession.read.text(inputDir)
      .select(functions.from_json($"value", EventLog.Schema).as("value"))
      .select("value.*")
      .withColumn("user", CleanserFunctionsStudy.anonymizeUserExtracted())
      .withColumn("technical", CleanserFunctionsStudy.cleanseBrowserExtracted())
      .withColumn("technical", CleanserFunctionsStudy.cleanseDeviceExtracted())
      .withColumn("technical", CleanserFunctionsStudy. cleanseNetworkExtracted())
      .withColumn("source", CleanserFunctionsStudy.cleanseSourceExtracted())
      .withColumn("value", functions.to_json(functions.struct("*")))
      .select("value")
      .write.mode("overwrite").text("/tmp/some_output")
  }

  def runOverriddenValueStructColumn(sparkSession: SparkSession, inputDir: String): Unit = {
    import sparkSession.implicits._
    sparkSession.read.text(inputDir)
      .select(functions.from_json($"value", EventLog.Schema).as("value"))
      .withColumn("value", $"value"
        .withField("technical.browser", CleanserFunctionsStudy.cleanseBrowserValue("name"))
        .withField("technical.lang", CleanserFunctionsStudy.cleanseBrowserValue("lang"))
        .withField("technical.device.type", CleanserFunctionsStudy.cleanseDeviceValue())
        .withField("technical.network", CleanserFunctionsStudy.cleanseNetworkValue())
      )
      .write.mode("overwrite").json("/tmp/some_output")
  }

  def runWithNewColumnStruct(sparkSession: SparkSession, inputDir: String) = {
    import sparkSession.implicits._
    sparkSession.read.text(inputDir)
      .select(functions.from_json($"value", EventLog.Schema).as("value"))
      .withColumn("cleansed_value", functions.struct(
        $"value.visit_id", $"value.user_id", $"value.event_time", $"value.page", $"value.keep_private",
        functions.struct(
          CleanserFunctionsStudy.cleanseSourceValue().as("site"), $"value.source.api_version"
        ).as("source"),
        functions.struct(
          CleanserFunctionsStudy.cleanseBrowserValue("name"),
          CleanserFunctionsStudy.cleanseBrowserValue("lang"),
          CleanserFunctionsStudy.cleanseDeviceValue(),
          CleanserFunctionsStudy.cleanseNetworkValue()
        ).as("technical")
      ))
      .select("cleansed_value.*")
      .write.mode("overwrite").json("/tmp/some_output")
  }

}
