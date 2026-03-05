# Spark Declarative Pipelines - 101

1. Prepare and start Docker container for Apache Kafka, Apache Spark Connect and Delta Lake:
```
mkdir -p ./data/checkpoint
mkdir -p ./data/warehouse
docker-compose up
```

2. Explain the [docker-compose.yaml](docker-compose.yaml)
* configured with `SPARK_NO_DAEMONIZE` to keep the container up and running
* enabled Delta Lake with the configuration and JAR dependencies
* exposes Spark Connect and Spark UI ports
* we also start Apache Kafka to show another feature of SDP

## Correct executions
3. Explain [sdp_materialized_view.py](sdp_materialized_view.py) and [sdp_materialized_view_spec.yaml](sdp_materialized_view_spec.yaml)
* the spec file
  * references the checkpoint location and the executed job; glob can include many files but for the sake
    of our demo, we insert only one
  * also specifies a few configurations
* the SDP job
  * the first SDP example with a materialized view that fully _materializes_ a batch dataset
  * also uses temporary view with an additional dataset but it could also be a transformation

4. Run the sdp_materialized_view:
```shell
spark-pipelines run --spec sdp_materialized_view_spec.yaml
```

5. Run `python sdp_materialized_view_read_table.py`. It should print:
```
in_memory_numbers
+---+------+
| id|letter|
+---+------+
|  1|     A|
|  3|     C|
|  2|     B|
|  6|     F|
|  4|     D|
|  5|     E|
+---+------+

text_letters_materialized_view
+-----+
|value|
+-----+
|    A|
|    B|
|    C|
+-----+
```

6. Explain [sdp_table_with_python_function.py](sdp_table_with_python_function.py)
* it's an example of the streaming table; a streaming (`readStream`) data source is allowed
* it also uses a regular Python function to decorate the processed `DataFrame`

7. Run the sdp_table_with_python_function:
```shell
spark-pipelines run --spec sdp_table_with_python_function_spec.yaml
```

8. Run `python sdp_table_with_python_function_read_table.py`. It should print:
```
rate_data_with_processing_time
+-------------------+-----+-----------------------+
|timestamp          |value|processing_time        |
+-------------------+-----+-----------------------+
|1970-01-01 00:00:00|0    |2026-02-26 05:54:57.823|
|1970-01-01 00:00:00|2    |2026-02-26 05:54:57.823|
|1970-01-01 00:00:00|4    |2026-02-26 05:54:57.823|
|1970-01-01 00:00:00|1    |2026-02-26 05:54:57.823|
|1970-01-01 00:00:00|3    |2026-02-26 05:54:57.823|
+-------------------+-----+-----------------------+
```

9. Explain [sdp_table_with_append_flow.py](sdp_table_with_append_flow.py)
* it does almost the same thing as `sdp_table_with_python_function.py` but instead of one stream, it has 2 sources
* it also creates a table outside the decorators and uses the `@pipelines.append_flow` to continuously add new records

10. Run the sdp_table_with_append_flow:
```shell
spark-pipelines run --spec sdp_table_with_append_flow_spec.yaml
```

11. Run `python sdp_table_with_append_flow_read_table.py`. It should print:
```
rate_table_append_flow
+-------------------+-----+
|timestamp          |value|
+-------------------+-----+
|1970-01-01 00:00:00|0    |
|1970-01-01 00:00:00|2    |
|1970-01-01 00:00:00|4    |
|2026-02-01 17:57:13|0    |
|2026-02-01 17:57:13|2    |
|2026-02-01 17:57:13|4    |
|1970-01-01 00:00:00|1    |
|1970-01-01 00:00:00|3    |
|2026-02-01 17:57:13|1    |
|2026-02-01 17:57:13|3    |
+-------------------+-----+
```

12. Explain the [sdp_kafka_sink.py](sdp_kafka_sink.py)
* here we use a dedicated Kafka sink instead of a table expression


13. Run the sdp_kafka_sink:
```shell
spark-pipelines run --spec sdp_kafka_sink_spec.yaml
```

14. Check the records in the topic:
```shell
docker exec kafka  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9094  --topic numbers --from-beginning
```

You should see:
```
A
B
C
```

## Errors
1. Discuss the retries by running the [errors_sdp_materialized_view.py](errors_sdp_materialized_view.py)
* the code breaks because the schema of the combined `DataFrame`s is different
2. Run the errors_sdp_materialized_view:
```shell
spark-pipelines run --spec errors_sdp_materialized_view_spec.yaml
``` 
You should see the job retrying a few times before giving up:
```
2026-03-05 04:48:17: Starting run...
2026-03-05 03:48:18: Flow spark_catalog.default.error_text_letters_materialized_view is QUEUED.
2026-03-05 03:48:18: Flow spark_catalog.default.error_in_memory_numbers is QUEUED.
2026-03-05 03:48:18: Flow spark_catalog.default.error_text_letters_materialized_view is PLANNING.
2026-03-05 03:48:18: Flow spark_catalog.default.error_text_letters_materialized_view is STARTING.
2026-03-05 03:48:18: Flow spark_catalog.default.error_text_letters_materialized_view is RUNNING.
2026-03-05 03:48:18: Flow spark_catalog.default.error_in_memory_numbers is PLANNING.
2026-03-05 03:48:18: Flow spark_catalog.default.error_in_memory_numbers is STARTING.
2026-03-05 03:48:18: Flow spark_catalog.default.error_in_memory_numbers is RUNNING.
2026-03-05 03:48:18: Flow 'spark_catalog.default.error_in_memory_numbers' has FAILED.
Error: [CAST_INVALID_INPUT] The value 'A' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
2026-03-05 03:48:19: Flow spark_catalog.default.error_text_letters_materialized_view has COMPLETED.
2026-03-05 03:48:24: Flow spark_catalog.default.error_in_memory_numbers is PLANNING.
2026-03-05 03:48:24: Flow spark_catalog.default.error_in_memory_numbers is STARTING.
2026-03-05 03:48:24: Flow spark_catalog.default.error_in_memory_numbers is RUNNING.
2026-03-05 03:48:24: Flow 'spark_catalog.default.error_in_memory_numbers' has FAILED.
Error: [CAST_INVALID_INPUT] The value 'A' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
2026-03-05 03:48:35: Flow spark_catalog.default.error_in_memory_numbers is PLANNING.
2026-03-05 03:48:35: Flow spark_catalog.default.error_in_memory_numbers is STARTING.
2026-03-05 03:48:35: Flow spark_catalog.default.error_in_memory_numbers is RUNNING.
2026-03-05 03:48:36: Flow 'spark_catalog.default.error_in_memory_numbers' has FAILED.
Error: [CAST_INVALID_INPUT] The value 'A' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
{"ts": "2026-03-05 04:48:38.065", "level": "ERROR", "logger": "SQLQueryContextLogger", "msg": "[CAST_INVALID_INPUT] The value 'A' of the type \"STRING\" cannot be cast to \"BIGINT\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018", "context": {"errorClass": "CAST_INVALID_INPUT"}, "exception": {"class": "_MultiThreadedRendezvous", "msg": "<_MultiThreadedRendezvous of RPC that terminated with:\n\tstatus = StatusCode.INTERNAL\n\tdetails = \"[CAST_INVALID_INPUT] The value 'A' of the type \"STRING\" cannot be cast to \"BIGINT\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018\"\n\tdebug_error_string = \"UNKNOWN:Error received from peer ipv4:127.0.0.1:15002 {grpc_status:13, grpc_message:\"[CAST_INVALID_INPUT] The value \\'A\\' of the type \\\"STRING\\\" cannot be cast to \\\"BIGINT\\\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018\"}\"\n>", "stacktrace": [{"class": null, "method": "_execute_and_fetch_as_iterator", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", "line": "1658"}, {"class": null, "method": "__next__", "file": "<frozen _collections_abc>", "line": "360"}, {"class": null, "method": "send", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/reattach.py", "line": "139"}, {"class": null, "method": "_has_next", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/reattach.py", "line": "191"}, {"class": null, "method": "_has_next", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/reattach.py", "line": "163"}, {"class": null, "method": "_call_iter", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/reattach.py", "line": "294"}, {"class": null, "method": "_call_iter", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/reattach.py", "line": "266"}, {"class": null, "method": "<lambda>", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/reattach.py", "line": "164"}, {"class": null, "method": "__next__", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/grpc/_channel.py", "line": "538"}, {"class": null, "method": "_next", "file": "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/grpc/_channel.py", "line": "956"}]}}
Traceback (most recent call last):
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/pipelines/cli.py", line 447, in <module>
    main()
    ~~~~^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/pipelines/cli.py", line 426, in main
    run(
    ~~~^
        spec_path=spec_path,
        ^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        dry=False,
        ^^^^^^^^^^
    )
    ^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/pipelines/cli.py", line 350, in run
    handle_pipeline_events(result_iter)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/pipelines/spark_connect_pipeline.py", line 53, in handle_pipeline_events
    for result in iter:
                  ^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1221, in execute_command_as_iterator
    for response in self._execute_and_fetch_as_iterator(req, observations or {}):
                    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1674, in _execute_and_fetch_as_iterator
    self._handle_error(error)
    ~~~~~~~~~~~~~~~~~~^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1982, in _handle_error
    self._handle_rpc_error(error)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 2066, in _handle_rpc_error
    raise convert_exception(
    ...<5 lines>...
    ) from None
pyspark.errors.exceptions.connect.NumberFormatException: [CAST_INVALID_INPUT] The value 'A' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
```
3. Explain the [errors_sdp_table_with_python_function.py](errors_sdp_table_with_python_function.py)
* here too we introduced a consistency error because streaming and batch DataFrames can't be unioned
* SDP runner will retry the flow a few times
4. Run the errors_sdp_table_with_python_function:
```shell
spark-pipelines run --spec errors_sdp_table_with_python_function_spec.yaml
```

You should see the job retrying:
```
2026-03-05 04:48:52: Starting run...
2026-03-05 03:48:52: Flow spark_catalog.default.error_rate_data_with_processing_time is QUEUED.
2026-03-05 03:48:52: Flow spark_catalog.default.error_rate_data_with_processing_time is STARTING.
2026-03-05 03:48:52: Flow 'spark_catalog.default.error_rate_data_with_processing_time' has FAILED.
Error: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#408, value#305L, cast(processing_time#306 as timestamp) AS processing_time#409]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]

2026-03-05 03:48:52: Flow 'spark_catalog.default.error_rate_data_with_processing_time' has FAILED.
Error: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#408, value#305L, cast(processing_time#306 as timestamp) AS processing_time#409]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]

2026-03-05 03:48:57: Flow spark_catalog.default.error_rate_data_with_processing_time is STARTING.
2026-03-05 03:48:57: Flow 'spark_catalog.default.error_rate_data_with_processing_time' has FAILED.
Error: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#413, value#305L, cast(processing_time#306 as timestamp) AS processing_time#414]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]

2026-03-05 03:48:57: Flow 'spark_catalog.default.error_rate_data_with_processing_time' has FAILED.
Error: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#413, value#305L, cast(processing_time#306 as timestamp) AS processing_time#414]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]

2026-03-05 03:49:07: Flow spark_catalog.default.error_rate_data_with_processing_time is STARTING.
2026-03-05 03:49:07: Flow 'spark_catalog.default.error_rate_data_with_processing_time' has FAILED.
Error: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#418, value#305L, cast(processing_time#306 as timestamp) AS processing_time#419]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]

2026-03-05 03:49:07: Flow 'spark_catalog.default.error_rate_data_with_processing_time' has FAILED more than 2 times and will not be restarted.
Error: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#418, value#305L, cast(processing_time#306 as timestamp) AS processing_time#419]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]

Traceback (most recent call last):
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/pipelines/cli.py", line 447, in <module>
    main()
    ~~~~^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/pipelines/cli.py", line 426, in main
    run(
    ~~~^
        spec_path=spec_path,
        ^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        dry=False,
        ^^^^^^^^^^
    )
    ^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/pipelines/cli.py", line 350, in run
    handle_pipeline_events(result_iter)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/pipelines/spark_connect_pipeline.py", line 53, in handle_pipeline_events
    for result in iter:
                  ^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1221, in execute_command_as_iterator
    for response in self._execute_and_fetch_as_iterator(req, observations or {}):
                    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1674, in _execute_and_fetch_as_iterator
    self._handle_error(error)
    ~~~~~~~~~~~~~~~~~~^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1982, in _handle_error
    self._handle_rpc_error(error)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^
  File "/Users/bartosz/_venvs/pyspark-4_1_0_with_pipelines/lib/python3.14/site-packages/pyspark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 2066, in _handle_rpc_error
    raise convert_exception(
    ...<5 lines>...
    ) from None
pyspark.errors.exceptions.connect.AnalysisException: Union between streaming and batch DataFrames/Datasets is not supported;
~Union false, false
:- ~Project [timestamp#292, value#293L, current_timestamp() AS processing_time#295]
:  +- ~StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@446e521d, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@48decae8, [rowsPerBatch=5, numPartitions=2], [timestamp#292, value#293L]
+- Project [cast(timestamp#307 as timestamp) AS timestamp#418, value#305L, cast(processing_time#306 as timestamp) AS processing_time#419]
   +- Project [timestamp#307, value#305L, processing_time#306]
      +- LocalRelation [value#305L, processing_time#306, timestamp#307]
```