# Stopping a Structured Streaming query with an external marker

1. Start the Kafka broker:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [VisitsCounterInWindows.scala](src%2Fmain%2Fscala%2Fcom%2Fwaitingforcode%2FVisitsCounterInWindows.scala)
* the job counts number of records for each visit
* besides, it starts a companion thread where it polls messages from the _markers_ Kafka topic
  * whenever it sees a message, it waits the current micro-batch to complete and soon after, it 
    stops the query
3. Start the Kafka producer for the markers topic:
```
docker exec -ti wfc_kafka kafka-console-producer.sh --topic markers --bootstrap-server localhost:9092
```

4. Run the `VisitsCounterInWindows`. After seeing the first "...processing a row" messages, add any message 
(e.g. 1) to the markers topic.

5. You should see the query stopped after the last micro-batch:

```
...processing a row
Received a marker, stopping the query now...99dfceb5-6f13-4832-965e-2cea006a276f
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
...processing a row
-------------------------------------------
Batch: 2
-------------------------------------------
+-----------------+-----+
|         visit_id|count|
+-----------------+-----+
|140180378897280_0|    5|
|140180378897280_1|    5|
|140180378897280_2|    5|
|140180378897280_3|    5|
|140180378897280_4|    5|
|140180378897280_5|    5|
|140180378897280_6|    5|
|140180378897280_7|    5|
|140180378897280_8|    5|
|140180378897280_9|    5|
+-----------------+-----+

Exception in thread "Thread-27" java.lang.InterruptedException
	at java.base/java.lang.Object.wait(Native Method)
	at java.base/java.lang.Thread.join(Thread.java:1313)
	at com.waitingforcode.VisitsCounterInWindows$.$anonfun$main$2(VisitsCounterInWindows.scala:59)
	at java.base/java.lang.Thread.run(Thread.java:834)
```
6. You can also uncomment the _sys.addShutdownHook {_ code and try to kill the job without using the 
marker event. Due to the race conditions with SparkContext's stop shutdown hook. You should see an exception like:

```
-------------------------------------------
Batch: 1
-------------------------------------------
+-----------------+-----+
|         visit_id|count|
+-----------------+-----+
|139979563846528_0|    2|
|139979563846528_1|    2|
|139979563846528_2|    2|
|139979563846528_3|    2|
|139979563846528_4|    2|
|139979563846528_5|    2|
|139979563846528_6|    2|
|139979563846528_7|    2|
|139979563846528_8|    2|
|139979563846528_9|    2|
+-----------------+-----+

...processing a row
...processing a row
SHUTDOWN!!!!!!!! ==> {
  "message" : "Processing new data",
  "isDataAvailable" : true,
  "isTriggerActive" : true
}
Exception in thread "shutdownHook1" java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext.
This stopped SparkContext was created at:

org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:1093)
com.waitingforcode.VisitsCounterInWindows$.main(VisitsCounterInWindows.scala:17)
com.waitingforcode.VisitsCounterInWindows.main(VisitsCounterInWindows.scala)

The currently active SparkContext was created at:

org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:1093)
com.waitingforcode.VisitsCounterInWindows$.main(VisitsCounterInWindows.scala:17)
com.waitingforcode.VisitsCounterInWindows.main(VisitsCounterInWindows.scala)
         
	at org.apache.spark.SparkContext.assertNotStopped(SparkContext.scala:122)
	at org.apache.spark.SparkContext.cancelJobGroup(SparkContext.scala:2577)
	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.stop(MicroBatchExecution.scala:207)
	at org.apache.spark.sql.execution.streaming.StreamingQueryWrapper.stop(StreamingQueryWrapper.scala:61)
	at com.waitingforcode.VisitsCounterInWindows$.$anonfun$main$3(VisitsCounterInWindows.scala:70)
	at scala.sys.ShutdownHookThread$.$anonfun$apply$1(ShutdownHookThread.scala:33)
	at java.base/java.lang.Thread.run(Thread.java:834)
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Job 2 cancelled because SparkContext was shut down
=== Streaming Query ===
Identifier: [id = e2708c7c-bd91-4404-ab26-2ecaa3696008, runId = b1872944-c94b-4910-bc6a-28fec4446ee3]
Current Committed Offsets: {KafkaV2[Subscribe[visits]]: {"visits":{"0":460}}}
Current Available Offsets: {KafkaV2[Subscribe[visits]]: {"visits":{"0":530}}}

Current State: ACTIVE
Thread State: RUNNABLE

Logical Plan:
WriteToMicroBatchDataSource org.apache.spark.sql.execution.streaming.ConsoleTable$@7f41d979, e2708c7c-bd91-4404-ab26-2ecaa3696008, Update
+- Aggregate [visit#21.visit_id], [visit#21.visit_id AS visit_id#24, count(1) AS count#26L]
   +- TypedFilter com.waitingforcode.VisitsCounterInWindows$$$Lambda$1458/0x0000000800c0c840@3149409c, interface org.apache.spark.sql.Row, [StructField(visit,StructType(StructField(visit_id,StringType,true)),true)], createexternalrow(if (isnull(visit#21)) null else createexternalrow(if (visit#21.isNullAt) null else visit#21.visit_id.toString, StructField(visit_id,StringType,true)), StructField(visit,StructType(StructField(visit_id,StringType,true)),true))
      +- Project [from_json(StructField(visit_id,StringType,true), cast(value#8 as string), Some(Europe/Paris)) AS visit#21]
         +- StreamingDataSourceV2Relation [key#7, value#8, topic#9, partition#10, offset#11L, timestamp#12, timestampType#13], org.apache.spark.sql.kafka010.KafkaSourceProvider$KafkaScan@6a5b988b, KafkaV2[Subscribe[visits]]

	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:332)
	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
Caused by: org.apache.spark.SparkException: Job 2 cancelled because SparkContext was shut down
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$cleanUpAfterSchedulerStop$1(DAGScheduler.scala:1248)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$cleanUpAfterSchedulerStop$1$adapted(DAGScheduler.scala:1246)
	at scala.collection.mutable.HashSet$Node.foreach(HashSet.scala:435)
	at scala.collection.mutable.HashSet.foreach(HashSet.scala:361)
	at org.apache.spark.scheduler.DAGScheduler.cleanUpAfterSchedulerStop(DAGScheduler.scala:1246)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onStop(DAGScheduler.scala:3075)
	at org.apache.spark.util.EventLoop.stop(EventLoop.scala:84)

```