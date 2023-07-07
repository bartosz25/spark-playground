# Setup
1. Start the Docker containers:
```
cd docker;
docker-compose down --volumes; docker-compose up
```
2. Create the topic:
```
docker exec -ti docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic visits --delete
docker exec -ti docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic visits --partitions 1 --create 
```
3. Start the producer:
```
docker exec -ti docker_kafka_1 kafka-console-producer.sh --broker-list localhost:29092 --topic visits
```

# Erroneous session expiration
1. Perform the setup steps.
2. Go to the producer and add:
```
{"userId": 1, "page": "index.html", "eventTime": "2023-06-06T10:00:00Z"}
```
3. Run the [FirstStateBrokenExpiration.scala](src%2Fmain%2Fscala%2Fcom%2Fbecomedataengineer%2FFirstStateBrokenExpiration.scala)
Expected output:
```
State will expire at 1970-01-01 02:00:00.0
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+
|value|
+-----+
+-----+

State timed out!
-------------------------------------------
Batch: 1
-------------------------------------------
+------+
|value |
+------+
|{1, 1}|
+------+
```
4. As you can see, our state expired! Let's see different strategies to handle that issue.

# Temporary state
1. Perform the setup.
2. Go to the producer and add:
```
{"userId": 1, "page": "index.html", "eventTime": "2023-06-06T10:00:00Z"}
```
3. Run the [StatefulJobWithFlagPattern.scala](src%2Fmain%2Fscala%2Fcom%2Fbecomedataengineer%2Ftemporarystate%2FStatefulJobWithFlagPattern.scala)
Expected output:
```
State will expire at 1970-01-01 00:02:00.0
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+
|value|
+-----+
+-----+

State timed out!
Setting new expiration time as at 2023-06-06 09:58:00.0
-------------------------------------------
Batch: 1
-------------------------------------------
+-----+
|value|
+-----+
+-----+
```
4. Add new record:
```
{"userId": 2, "page": "home.html", "eventTime": "2023-06-06T10:01:00Z"}
```

5. Nothing should happen:
```
State will expire at 2023-06-06 09:58:00.0
-------------------------------------------
Batch: 2
-------------------------------------------
+-----+
|value|
+-----+
+-----+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+
|value|
+-----+
+-----+
```
6. Add new record to advance the watermark:
```
{"userId": 3, "page": "home.html", "eventTime": "2023-06-06T10:05:00Z"}
```
7. This time, the state created in the first execution should expire alongside the others:
```
State will expire at 2023-06-06 09:59:00.0
-------------------------------------------
Batch: 4
-------------------------------------------
+-----+
|value|
+-----+
+-----+

State timed out!
State timed out!
State timed out!
-------------------------------------------
Batch: 5
-------------------------------------------
+------+
|value |
+------+
|{2, 1}|
|{3, 1}|
|{1, 1}|
+------+
```

The state **3** expires too because the expiration time falls behind the watermark. The configured 
state TTL is smaller than the watermark delay.

# Event-time strategy
1. Perform the setup.
2. Go to the producer and add:
```
{"userId": 1, "page": "index.html", "eventTime": "2023-06-06T10:00:00Z"}
```
3. Run the [StatefulJobWithEventTimePattern.scala](src%2Fmain%2Fscala%2Fcom%2Fbecomedataengineer%2Feventtime%2FStatefulJobWithEventTimePattern.scala)
Expected output:
```
State will expire at 2023-06-06 10:02:00.0
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+
|value|
+-----+
+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+
|value|
+-----+
+-----+
```
4. Add new record:
```
{"userId": 2, "page": "home.html", "eventTime": "2023-06-06T10:01:00Z"}
```

5. Nothing should happen:
```
State will expire at 2023-06-06 10:03:00.0
-------------------------------------------
Batch: 2
-------------------------------------------
+-----+
|value|
+-----+
+-----+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+
|value|
+-----+
+-----+

```
6. Add new record to advance the watermark:
```
{"userId": 3, "page": "home.html", "eventTime": "2023-06-06T10:05:00Z"}
```
7. Unlike previously, the watermark didn't advance enough to trigger the GC Watermark:
```
State will expire at 2023-06-06 10:07:00.0
-------------------------------------------
Batch: 4
-------------------------------------------
+-----+
|value|
+-----+
+-----+

-------------------------------------------
Batch: 5
-------------------------------------------
+-----+
|value|
+-----+
+-----+
```
8. Let's add anothe record:
```
{"userId": 4, "page": "home.html", "eventTime": "2023-06-06T10:07:00Z"}
```
9. This time the state for the user 1 expired:
```
State will expire at 2023-06-06 10:09:00.0
-------------------------------------------
Batch: 6
-------------------------------------------
+-----+
|value|
+-----+
+-----+

State timed out!
-------------------------------------------
Batch: 7
-------------------------------------------
+------+
|value |
+------+
|{1, 1}|
+------+
```