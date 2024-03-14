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