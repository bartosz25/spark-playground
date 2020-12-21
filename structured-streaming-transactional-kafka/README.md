# Transactional writer
1. Start the broker:
```
cd broker
docker-compose down --volumes; docker-compose up
```

2. Start [TestDataGenerator](src/main/scala/com/waitingforcode/TestDataGenerator.scala)
   (like any other JVM process; e.g. use 'Right Click + Run as ...' if you work with IntelliJ)
   
3. Start [ForeachKafkaTransactionalDemo](src/main/scala/com/waitingforcode/ForeachKafkaTransactionalDemo.scala)
   (see the point 2)

4. Produce all records until the `ForeachKafkaTransactionalWriter` fails.
   `ForeachKafkaTransactionalWriter` should fail when the letter 'k' is processed.

5. Change `val ShouldFailOnK = true` to `val ShouldFailOnK = false` in
   [waitingforcode package](src/main/scala/com/waitingforcode/package.scala)

6. Restart the [ForeachKafkaTransactionalDemo](src/main/scala/com/waitingforcode/ForeachKafkaTransactionalDemo.scala)
   (see the point 2)

7. Resume records generation.

8. Consume all messages generated to the topic with `docker exec -ti broker_kafka_1 kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic transactional_kafka_demo_output --from-beginning`

**Expected**:
TODO

# Non transactional writer
Follow the same steps as for the transactional except that:
* use [ForeachKafkaNonTransactionalDemo](src/main/scala/com/waitingforcode/ForeachKafkaNonTransactionalDemo.scala) instead of 
`ForeachKafkaTransactionalDemo`

* Execute `docker exec -ti broker_kafka_1 kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic non_transactional_kafka_demo_output --from-beginning` in the step 8.

# Useful commands
* To delete the checkpoint files: `rm -rf /tmp/waitingforcode/transactional_kafka_demo_output/checkpoint/`
* To delete the committed transactions store: `rm -rf /tmp/waitingforcode/kafka-transactions-store`