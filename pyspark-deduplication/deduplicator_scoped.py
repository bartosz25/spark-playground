import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from in_memory_data_source import InMemoryDataSource, InMemoryDataSourceStreamHolder

os.environ['TZ'] = 'UTC'
time.tzset()
spark = SparkSession.builder.master('local[*]').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
spark.dataSource.register(InMemoryDataSource)
input_data_stream = spark.readStream.format('in_memory').load()


deduplicated_visits = (input_data_stream
                       .withWatermark('event_time', '10 minutes')
                       .dropDuplicates(['visit_id', 'event_time']))

write_data_stream = (deduplicated_visits.writeStream#.trigger(availableNow=True)
                     .outputMode('update').format('console'))

write_data_query = write_data_stream.start()
def event_time(minutes: int):
    return f'2025-10-20T09:{minutes}:00.000Z'


InMemoryDataSourceStreamHolder.write_records([Row(visit_id=1, event_time=event_time(10)).asDict(),
          Row(visit_id=2, event_time=event_time(10)).asDict()])
write_data_query.processAllAvailable()
print(f'Watermark is: {write_data_query.lastProgress["eventTime"]["watermark"]}')


InMemoryDataSourceStreamHolder.write_records([Row(visit_id=1, event_time=event_time(10)).asDict(),
          Row(visit_id=2, event_time=event_time(25)).asDict()])
write_data_query.processAllAvailable()

print(f'Watermark is: {write_data_query.lastProgress["eventTime"]["watermark"]}')

InMemoryDataSourceStreamHolder.write_records([Row(visit_id=1, event_time=event_time(10)).asDict(),
          Row(visit_id=100, event_time=event_time(10)).asDict(),
          Row(visit_id=3, event_time=event_time(26)).asDict()])
write_data_query.processAllAvailable()
print(f'Watermark: {write_data_query.lastProgress["eventTime"]["watermark"]}')