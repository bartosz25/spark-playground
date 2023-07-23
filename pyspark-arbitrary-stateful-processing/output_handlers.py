import dataclasses
from typing import Tuple, Any, Dict

from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from state_handlers import StateSchemaHandler


class OutputHandler:

    def __init__(self, group_id: StructField, start_time: StructField, end_time: StructField,
                 duration_in_milliseconds: StructField, is_final: StructField,
                 state_schema_handler: StateSchemaHandler):
        self.group_id = group_id
        self.start_time = start_time
        self.end_time = end_time
        self.duration_in_milliseconds = duration_in_milliseconds
        self.is_final = is_final
        self.schema = StructType([
            self.group_id, self.start_time, self.end_time,
            self.duration_in_milliseconds, self.is_final
        ])
        self.state_schema_handler = state_schema_handler

    def generate_output(self, group_id: int, timed_out_state: bool, state_dict: Dict[str, Any]) -> Dict[str, Any]:
        start_time_for_output = self.state_schema_handler.start_time.get(state_dict)
        end_time_for_output = self.state_schema_handler.end_time.get(state_dict)
        duration_in_milliseconds = (end_time_for_output - start_time_for_output).total_seconds() * 1000
        return {
            self.group_id.name: [group_id],
            self.start_time.name: [start_time_for_output.isoformat()],
            self.end_time.name: [end_time_for_output.isoformat()],
            self.duration_in_milliseconds.name: [int(duration_in_milliseconds)],
            self.is_final.name: [timed_out_state]
        }
