1. Start the broker: 
```
cd broker
docker-compose down; docker-compose up
```
2. Recreate the topic:
```
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic rocksdb_state_store --delete
docker exec -ti broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic rocksdb_state_store --create
```
3. Start the producer:
```
docker exec -ti broker_kafka_1 kafka-console-producer.sh --bootstrap-server localhost:29092 --topic rocksdb_state_store
```
4. Send the events for the first micro-batch:
```
{"event_time": "2021-10-05T01:20:00", "id": 1}
{"event_time": "2021-10-05T01:26:00", "id": 2}
{"event_time": "2021-10-05T01:20:00", "id": 1}
```
5. Check the checkpoint location:
```
tree /tmp/wfc/structured-streaming-rocksdb
```
6. Send the events for another micro-batch:
```
{"event_time": "2021-10-05T01:26:00", "id": 3}
{"event_time": "2021-10-05T03:25:00", "id": 2}
```
7. Check the checkpoint location:
```
tree /tmp/wfc/structured-streaming-rocksdb
```

