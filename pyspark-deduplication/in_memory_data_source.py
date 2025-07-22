import json
from datetime import datetime
from typing import Union, Iterator, Tuple, List

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType, Row


class InMemoryDataSourceStreamHolder:
    FILE_NAME = '/tmp/visits.json'
    OFFSETS = 0

    @staticmethod
    def write_records(records: list[dict]):
        print(f'Saving {records}')
        with open(InMemoryDataSourceStreamHolder.FILE_NAME, 'w', encoding='utf-8') as json_file:
            json_file.write(str(InMemoryDataSourceStreamHolder.OFFSETS))
            json_file.write('\n')
            for r in records:
                json_file.write(json.dumps(r))
                json_file.write('\n')
                InMemoryDataSourceStreamHolder.OFFSETS += 1

    @staticmethod
    def read_records() -> list[dict]:
        with open(InMemoryDataSourceStreamHolder.FILE_NAME, 'r', encoding='utf-8') as json_file:
            for line in json_file.readlines():
                if line.startswith('{'):
                    yield json.loads(line)

class InMemoryDataSourceStreamReader(DataSourceStreamReader):

    def __init__(self):
        self.records_read = 0
        self.records = []

    def initialOffset(self) -> dict:
        return {'offset': 0}

    def latestOffset(self) -> dict:
        with open(InMemoryDataSourceStreamHolder.FILE_NAME, 'r', encoding='utf-8') as json_file:
            l = json_file.readline()
            offset_number = int(l)
        # Note for myself: this latestOffset has to increase. Otherwise, Spark stops the query
        # as it doesn't see the progress.
        return {'offset': offset_number}

    def partitions(self, start, end) -> List[InputPartition]:
        return [InputPartition(None)]

    def read(self, partition: InputPartition) -> Union[Iterator[Tuple], Iterator["RecordBatch"]]:
        for record in InMemoryDataSourceStreamHolder.read_records():
            self.records_read += 1
            yield Row(visit_id=record['visit_id'],
                      event_time=datetime.strptime(record['event_time'], "%Y-%m-%dT%H:%M:%S.%fZ"))


class InMemoryDataSource(DataSource):

    @classmethod
    def name(cls):
        return 'in_memory'

    def schema(self):
        return "visit_id INTEGER, event_time TIMESTAMP"

    def reader(self, schema: StructType):
        raise NotImplementedError('Method not implemented yet')

    def streamReader(self, schema) -> InMemoryDataSourceStreamReader:
        return InMemoryDataSourceStreamReader()
