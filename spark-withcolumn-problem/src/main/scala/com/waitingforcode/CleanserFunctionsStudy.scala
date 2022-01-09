package com.waitingforcode

import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.sql.{Column, functions}

object CleanserFunctionsStudy {

  // Use value.
  def cleanseBrowser(): Column = {
    functions.when(col("value.technical.browser").startsWith("{"),
      col("value").withField("technical.browser", get_json_object(col("value.technical.browser"), "$.name"))
        .withField("technical.lang", get_json_object(col("value.technical.browser"), "$.language"))
    ).otherwise(col("value"))
  }
  def cleanseDevice(): Column = {
    functions.when(col("value.technical.device.type").startsWith("{"),
      col("value").withField("technical.device.type", get_json_object(col("value.technical.device.type"), "$.name"))
    ).otherwise(col("value"))
  }
  def cleanseNetwork(): Column = {
    functions.when(col("value.technical.network").startsWith("{"),
      col("value").withField("technical.network", get_json_object(col("value.technical.network"), "$.long_name"))
    ).otherwise(col("value"))
  }
  def cleanseSource(): Column = {
    val wwwPrefix = "www."
    functions.when(col("value.source.site").startsWith(wwwPrefix),
      col("value").withField("source.site", functions.ltrim(col("value.source.site"), wwwPrefix))
    ).otherwise(col("value"))
  }
  def anonymizeUser(): Column = {
    functions.when(functions.col("value.keep_private").eqNullSafe(true),
      functions.col("value").withField("user", functions.lit(null: String)))
      .otherwise(functions.col("value"))
  }

  // Use extracted columns
  def cleanseBrowserExtracted(): Column = {
    functions.when(col("technical.browser").startsWith("{"),
      col("technical").withField("browser", get_json_object(col("technical.browser"), "$.name"))
        .withField("lang", get_json_object(col("technical.browser"), "$.language"))
    ).otherwise(col("technical"))
  }
  def cleanseDeviceExtracted(): Column = {
    functions.when(col("technical.device.type").startsWith("{"),
      col("technical").withField("device.type", get_json_object(col("technical.device.type"), "$.name"))
    ).otherwise(col("technical"))
  }
  def cleanseNetworkExtracted(): Column = {
    functions.when(col("technical.network").startsWith("{"),
      col("technical").withField("network", get_json_object(col("technical.network"), "$.long_name"))
    ).otherwise(col("technical"))
  }
  def cleanseSourceExtracted(): Column = {
    val wwwPrefix = "www."
    functions.when(col("source.site").startsWith(wwwPrefix),
      col("source").withField("site", functions.ltrim(col("source.site"), wwwPrefix))
    ).otherwise(col("source"))
  }
  def anonymizeUserExtracted(): Column = {
    functions.when(functions.col("keep_private").eqNullSafe(true),
      functions.lit(null: String))
      .otherwise(functions.col("user"))
  }

  // Returns value
  def cleanseBrowserValue(t: String): Column = {
    if (t == "name") {
      functions.when(col("value.technical.browser").startsWith("{"),
        get_json_object(col("value.technical.browser"), "$.name")
      ).otherwise(col("value.technical.browser"))
    } else {
      functions.when(col("value.technical.browser").startsWith("{"),
        get_json_object(col("value.technical.browser"), "$.language")
      ).otherwise(col("value.technical.lang"))
    }
  }

  def cleanseDeviceValue(): Column = {
    functions.when(col("value.technical.device.type").startsWith("{"),
      get_json_object(col("value.technical.device.type"), "$.name")
    ).otherwise(col("value.technical.device.type"))
  }

  def cleanseNetworkValue(): Column = {
    functions.when(col("value.technical.network").startsWith("{"),
      get_json_object(col("value.technical.network"), "$.long_name")
    ).otherwise(col("value.technical.network"))
  }

  def cleanseSourceValue(): Column = {
    val wwwPrefix = "www."
    functions.when(col("value.source.site").startsWith(wwwPrefix),
      functions.ltrim(col("value.source.site"), wwwPrefix)
    ).otherwise(col("value.source.site"))
  }

}
