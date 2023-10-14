package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SparkSession, functions}

import java.io.File
import java.sql.Timestamp


object WatermarkPropagation {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      // already true by default, here to highlight the new config
      .config("spark.sql.streaming.statefulOperator.allowMultiple", true)
      .config("spark.sql.streaming.noDataMicroBatches.enabled", false)
      .getOrCreate()

    import sparkSession.implicits._
    implicit val sparkContext = sparkSession.sqlContext
    val clicksStream = MemoryStream[Click]
    val impressionsStream = MemoryStream[Impression]

    val clicksWithWatermark = clicksStream.toDF
      .withWatermark("clickTime", "10 minutes")
    val impressionsWithWatermark = impressionsStream.toDF
      .withWatermark("impressionTime", "20 minutes")

    val joined = impressionsWithWatermark.join(
      clicksWithWatermark,
      functions.expr("""clickAdId = impressionAdId AND
                  clickTime >= impressionTime AND
                  clickTime <= impressionTime + interval 2 minutes
                  """),
      joinType = "leftOuter"
    )

    val groups = joined
      .groupBy(functions.window($"clickTime", "1 hour"))
      .count()

    FileUtils.deleteDirectory(new File("/tmp/checkpoints"))
    val query = groups.writeStream.format("console").option("truncate", false)
      .option("checkpointLocation", s"/tmp/checkpoints")
      .start()

    clicksStream.addData(Seq(
      Click(1, Timestamp.valueOf("2023-06-10 10:10:00")),
      Click(1, Timestamp.valueOf("2023-06-10 10:20:00")),
      Click(2, Timestamp.valueOf("2023-06-10 10:30:00"))
    ))
    impressionsStream.addData(Seq(
      Impression(1, Timestamp.valueOf("2023-06-10 10:00:00"))
    ))


    query.processAllAvailable()
    // query.explain(true)
    //println(query.lastProgress.prettyJson)


    clicksStream.addData(Seq(
      Click(4, Timestamp.valueOf("2023-06-10 11:01:00")),
      Click(1, Timestamp.valueOf("2023-06-10 11:03:00")),
      Click(2, Timestamp.valueOf("2023-06-10 11:04:00"))
    ))
    impressionsStream.addData(Seq(
      Impression(5, Timestamp.valueOf("2023-06-10 11:00:00"))
    ))
    query.processAllAvailable()

sys.exit(1)
    impressionsStream.addData(Seq(
      Impression(5, Timestamp.valueOf("2023-06-10 11:00:00"))
    ))
    query.processAllAvailable()

    Thread.sleep(500000L)
  }

}

case class Click(clickAdId: Int, clickTime: Timestamp)
case class Impression(impressionAdId: Int, impressionTime: Timestamp)