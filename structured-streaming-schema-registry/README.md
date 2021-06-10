Demo steps for:
- schema evolution part
- micro-batch semantic part

1. Start Docker containers: 
```
cd broker
docker-compose down --volumes
docker-compose up
```

2. Create topics:
```
docker exec -ti broker kafka-topics --bootstrap-server localhost:29092 --topic orders --create
```

3. Open a new terminal tab and register the first version of the schema:
```
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.waitingforcode\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\", \"null\"], \"default\": null},{\"name\":\"amount\",\"type\":\"double\"}]}"}'   http://localhost:8081/subjects/orders-value/versions
```
4. Start `AbrisSchemaRegistryDemo` in `Debug mode`.

5. Start `ProducerAppV1`

6. Observe what happens in the streaming job:

7. Stop the producer and evolve the schema:
``` 
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json"   --data '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.waitingforcode\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\", \"null\"], \"default\": null},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"vat\",\"type\":[\"null\", \"double\"], \"default\": null}]}"}'   http://localhost:8081/subjects/orders-value/versions
```

8. Start `ProducerAppV2`: 

9. Observe the streaming job