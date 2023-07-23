import dataclasses
from typing import Tuple, Any, Dict

from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


@dataclasses.dataclass
class StructFieldWithStateUpdateHandler:
    field: StructField

    def get(self, state_dict_to_read: Dict[str, Any]) -> Any:
        return state_dict_to_read[self.field.name]

    def update(self, state_dict_to_update: Dict[str, Any], new_value: Any):
        state_dict_to_update[self.field.name] = new_value


class StateSchemaHandler:

    def __init__(self, start_time: StructFieldWithStateUpdateHandler,
                 end_time: StructFieldWithStateUpdateHandler):
        self.start_time = start_time
        self.end_time = end_time
        self.schema = StructType([
            self.start_time.field, self.end_time.field
        ])

    # Problem#1 ==> state is returned as a tuple, may be difficult or error-prone to use it
    def get_state_as_dict(self, state_tuple: Tuple) -> Dict[str, Any]:
        return dict(zip(self.schema.fieldNames(), state_tuple))

    def get_empty_state_dict(self) -> Dict[str, Any]:
        field_names = self.schema.fieldNames()
        return {field_names[i]: None for i in range(0, len(field_names))}

    # Problem#2 ==> Update requires a tuple; we have a dict, why not converting it back?
    @staticmethod
    def transform_in_flight_state_to_state_to_write(in_flight_state: Dict[str, Any]) -> Tuple:
        return tuple(in_flight_state.values())
