# *MapGroupsWithState and batch processing

1. Explain the [StatefulMappingFunction.scala](src%2Fmain%2Fscala%2Fcom%2Fwaitingforcode%2FStatefulMappingFunction.scala)
* it's the state mapping function; it doesn't have any complex logic as the goal is to show its interaction with the 
  mapState functions
2. Explain the [FlatMapGroupsWithStateBatch.scala](src%2Fmain%2Fscala%2Fcom%2Fwaitingforcode%2FFlatMapGroupsWithStateBatch.scala)
* the job will map all rows in a group following the similar declaration mode as for Structured Streaming
3. Run the `FlatMapGroupsWithStateBatch`. It should fail with the following exception:
```
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 2.0 failed 1 times, most recent failure: Lost task 0.0 in stage 2.0 (TID 6) (192.168.1.55 executor driver): org.apache.spark.SparkUnsupportedOperationException: Cannot get event time watermark timestamp without setting watermark before [map|flatMap]GroupsWithState.
	at org.apache.spark.sql.errors.QueryExecutionErrors$.cannotGetEventTimeWatermarkError(QueryExecutionErrors.scala:1835)
	at org.apache.spark.sql.execution.streaming.GroupStateImpl.getCurrentWatermarkMs(GroupStateImpl.scala:142)
	at com.waitingforcode.StatefulMappingFunction$.concatenateRowsInGroup(StatefulMappingFunction.scala:10)
```

The job doesn't keep the watermark despite its initial declaration. Below is the execution plan:
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false, true) AS value#16]
   +- MapGroups org.apache.spark.sql.execution.MapGroupsExec$$$Lambda$1837/0x0000000800f06040@dd3e1e3, value#11: int, newInstance(class com.waitingforcode.TimestampedEvent), [value#11], [eventId#2, eventTime#3], obj#14: java.lang.String
      +- Sort [value#11 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(value#11, 1), ENSURE_REQUIREMENTS, [plan_id=17]
            +- AppendColumns com.waitingforcode.FlatMapGroupsWithStateBatch$$$Lambda$1639/0x0000000800dfd840@7804a783, newInstance(class com.waitingforcode.TimestampedEvent), [input[0, int, false] AS value#11]
               +- LocalTableScan [eventId#2, eventTime#3]
```

4. Comment this line in the `StatefulMappingFunction`:
```
println(s"State is=${state.getCurrentWatermarkMs()}")
```

5. Run the `FlatMapGroupsWithStateBatch` again. This time it should work and map the groups:

```
State is=GroupState(<undefined>)
State is=GroupState(<undefined>)
State is=GroupState(<undefined>)
State is=GroupState(<undefined>)
+---------------------+
|value                |
+---------------------+
|2024-04-01 09:00:00.0|
|2024-04-01 09:00:00.0|
|2024-04-01 09:02:00.0|
|2024-04-01 10:02:50.0|
|2024-04-01 09:04:00.0|
|2024-04-01 09:12:50.0|
+---------------------+
```

As you can notice, there is no state. However, you can set it if you define the `initialState`.

6. Explain the [FlatMapGroupsWithStateAndInitBatch.scala](src%2Fmain%2Fscala%2Fcom%2Fwaitingforcode%2FFlatMapGroupsWithStateAndInitBatch.scala)
* this job sets the initial state for the mapping function; the state must share the same key as the mapped DataFrame

7. Run the `FlatMapGroupsWithStateAndInitBatch`. This time, it should print the state info:

```
State is=GroupState(List(init for 1: 1=1))
State is=GroupState(<undefined>)
State is=GroupState(<undefined>)
State is=GroupState(<undefined>)
State is=GroupState(List(init for 10: 10=10))
+---------------------+
|value                |
+---------------------+
|init for 1: 1=1      |
|2024-04-01 09:00:00.0|
|2024-04-01 09:00:00.0|
|2024-04-01 09:02:00.0|
|2024-04-01 10:02:50.0|
|2024-04-01 09:04:00.0|
|2024-04-01 09:12:50.0|
|init for 10: 10=10   |
+---------------------+
```