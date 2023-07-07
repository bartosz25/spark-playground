package com.becomedataengineer.temporarystate

import com.becomedataengineer.{UserWithVisits, VisitTimeAndPage}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession, functions}

import java.util.TimeZone
import java.util.concurrent.TimeUnit

object StatefulJobWithFlagPattern {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    import sparkSession.implicits._
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val dataFrame = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "visits")
      .option("startingOffsets", "EARLIEST")
      .load()
      .select(functions.from_json($"value".cast("string"), VisitTimeAndPage.Schema).as("value"))
      .select($"value.*")

    val sessionTimeout = TimeUnit.MINUTES.toMillis(2)
    val query: KeyValueGroupedDataset[Int, VisitTimeAndPage] = dataFrame
      .withWatermark("eventTime", "4 minutes")
      .as[VisitTimeAndPage]
      .groupByKey(visit => visit.userId)

    val statefulQuery: Dataset[Option[UserWithVisits]] = query.mapGroupsWithState(
      GroupStateTimeout.EventTimeTimeout()
    )(
      FlagStateMapper.countItemsForLabel(sessionTimeout)
    )


    val writeQuery = statefulQuery
      .filter(finalOutput => finalOutput.isDefined)
      .writeStream.outputMode(OutputMode.Update())
      .format("console").option("truncate", "false")
      .start()

    writeQuery.awaitTermination()
  }

}