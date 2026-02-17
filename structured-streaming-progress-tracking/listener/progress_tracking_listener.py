import json
from dataclasses import dataclass

from kafka import KafkaProducer
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent


@dataclass
class ProgressTrackingListener(StreamingQueryListener):

    output_topic: str
    job_name: str
    broker_host_port: str

    def __post_init__(self):
        self.producer = KafkaProducer(bootstrap_servers=self.broker_host_port)

    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        pass

    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        for source_progress in event.progress.sources:
                parse_result = json.dumps({'job': self.job_name,
                                           'last_offsets': json.loads(source_progress.endOffset),
                                           'input_rows': source_progress.numInputRows})
                if parse_result:
                    self.producer.send(self.output_topic, parse_result.encode('utf-8'))
        self.producer.flush()

    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        pass