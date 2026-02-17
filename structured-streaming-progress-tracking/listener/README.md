# Progress tracking - listener

1. Setup the directories and Kafka broker:
```commandline
rm -rf /tmp/wfc/spark-playground/structured-streaming-progress-tracking/
mkdir -p /tmp/wfc/spark-playground/structured-streaming-progress-tracking/

docker-compose down --volumes; docker-compose up
```

2. Generate the first set of rows:
```
python generate_new_rows.py
```

3. Explain the [read_visits.py](read_visits.py)
* the job continuously streams the changes and prints the records to the console
* a more important part is the listener ([progress_tracking_listener.py](progress_tracking_listener.py)) that intercepts the modifications
  and sends them to the _observability_ topic

4. Start the job:
```
python read_visits.py
```

5. Run `python read_observability_topic.py`. You should see the monitored events:
```
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                               |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{"job": "read_visits", "last_offsets": {"sourceVersion": 1, "reservoirId": "870038f6-e50d-4ed7-9d7a-83ae944aee80", "reservoirVersion": 1, "index": -1, "isStartingVersion": false}, "input_rows": 4}|
|{"job": "read_visits", "last_offsets": {"visits": {"0": 4}}, "input_rows": 0}                                                                                                                       |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
