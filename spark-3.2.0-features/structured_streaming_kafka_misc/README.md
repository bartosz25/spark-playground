1. Start the broker: 
```
cd broker
docker-compose down; docker-compose up
```
2. Recreate the topic:
```
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic kafka_changes --delete
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic kafka_changes --create
```
3. Start the producer:
```
docker exec -ti broker_kafka_1 kafka-console-producer.sh --bootstrap-server localhost:29092 --topic kafka_changes
```
4. Run the `KafkaChangesDataLossConfigurationDemo` twice, once for each `startingOffsetsByTimestampStrategy` configuration option.

5. Run the `KafkaChangesMinOffsetsMaxTriggerDemo`

6. Send the events for the first micro-batch:
```
1
2
3
4
5
```
📝 The micro-batch should run yet.

7. Now produce 4  records and see if the micro-batch sink executed:
```
6
7
8
9
```
8. Add an extra record to see the 4 previous ones process:
```
10
```
9. Go to https://www.epochconverter.com/ and find the date corresponding to sending the previous events. 
*The broker runs in UTC timezone!*
10. Set the timestamp to `KafkaChangesStartingTimestampDemo`
11. Start the `KafkaChangesStartingTimestampDemo`. You should see all the previously delivered events.
12. Add some new events in the console producer:
```
11
12
```
13. Wait some time and get the timestamp corresponding to the previous events delivery from https://www.epochconverter.com/
14. Set the timestamp to `KafkaChangesStartingTimestampDemo` and restart the processing.
15. You should only see the events `11, 12`
16. Start now the `KafkaLatencyMetricsDemo`
17. Add the records:
```
a
b
c
d
e
f
g
h
i
j
k
l
```
18. Check the logs and look for the *BehindLatest metrics:
```
less /tmp/structured-streaming-3.2.0/main/log.out
// look for BehindLatest
``` 