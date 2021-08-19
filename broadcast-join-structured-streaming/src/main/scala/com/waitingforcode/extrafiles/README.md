# Demo steps

## Common steps before each demo
1. Start Kafka broker if it's not running
2. Delete the demo topic: `docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic broadcast_join_demo_new_file_created --delete`
3. Create the demo topic: `docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic broadcast_join_demo_new_file_created --partitions 2 --create`
4. Start the producer: `docker exec -ti broker_kafka_1 kafka-console-producer.sh --broker-list localhost:29092 --topic broadcast_join_demo_new_file_created`
5. Run the `StaticDataWriterV1` class from the tested package (json or delta).

## Not working batch
1. Start the `json.NotWorkingBatchApp`
2. Type _2_ in the Kafka producer console
3. You should see a match in the streaming job output
4. Run `StaticDataWriterV2' class
5. Type _12_ in the Kafka producer console
6. You should see a row with the empty right side, despite the fact of creating a new file with the matching key!

## Temporary table
1. Start the `json.TemporaryTableApp`
2. Type _2_ in the Kafka producer console
3. You should see a match in the streaming job output
4. Run `StaticDataWriterV2' class and wait for the refresh output from the background thread.
5. Type _12_ in the Kafka producer console
6. You should see a matched row

## Delta Lake batch join
1. Start the `delta.DeltaLakeJoinApp`
2. Type _2_ in the Kafka producer console
3. You should see a match in the streaming job output
4. Run `StaticDataWriterV2' class.
5. Type _12_ in the Kafka producer console
6. You should see a matched row

