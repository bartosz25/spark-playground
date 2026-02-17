# Progress tracking - checkpoint job

1. Setup the directories:
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
* the job also uses a checkpoint location where the checkpoint reader will retrieve progress data

4. Start the job:
```
python read_visits.py
```

5. Add new set of rows `python generate_new_rows.py`

6. Run `python process_checkpoint_progress.py`. You should see the monitored events:
```
+----------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+
|value                                                                                                                             |micro_batch_version|progress_source|
+----------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+
|{"sourceVersion":1,"reservoirId":"0bc7f5f0-8caf-4754-a7a5-2f28026b8d56","reservoirVersion":1,"index":-1,"isStartingVersion":false}|0                  |Delta Lake     |
|{"visits":{"0":4}}                                                                                                                |0                  |Apache Kafka   |
|{"sourceVersion":1,"reservoirId":"0bc7f5f0-8caf-4754-a7a5-2f28026b8d56","reservoirVersion":2,"index":-1,"isStartingVersion":false}|1                  |Delta Lake     |
|{"visits":{"0":8}}                                                                                                                |1                  |Apache Kafka   |
+----------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+
```
