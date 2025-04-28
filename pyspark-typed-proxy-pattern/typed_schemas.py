import dataclasses
from functools import reduce
from dataclasses import dataclass
from typing import Any, List

from pyspark.sql import Column
from pyspark.sql import functions
from pyspark.sql.types import StringType, StructField, DataType, StructType, Row


@dataclass(kw_only=True)
class SchemaAttribute:
    name: str
    type: DataType

    def as_column(self) -> Column:
        return functions.col(self.name)

    def as_field(self) -> StructField:
        return StructField(self.name, self.type)

    def for_row(self, values: List | Any) -> (str, [List | Any]):
        print(f'{self.name}={values} // {isinstance(values, list)}')
        if not isinstance(values, list):
            return {self.name: values}
        entries = reduce(lambda union, next_dict: union.update(next_dict) or union, values, {})
        return {
            self.name: entries
        }

@dataclass(kw_only=True)
class CitySchemaAttribute(SchemaAttribute):
    city_name = SchemaAttribute(name='city_name', type=StringType())
    zip_code = SchemaAttribute(name='zip_code', type=StringType())

    def as_field(self) -> StructField:
        return StructField(self.name, StructType(fields=[self.city_name.as_field(), self.zip_code.as_field()]))

@dataclass(kw_only=True)
class AddressSchemaAttribute(SchemaAttribute):
    street_name = SchemaAttribute(name='street_name', type=StringType())
    city = CitySchemaAttribute(name='city', type=StructType())

    def as_field(self) -> StructField:
        print('Getting field!')
        return StructField(self.name, StructType(fields=[self.street_name.as_field(), self.city.as_field()]))

def create_spark_schema_for_typed_schema(typed_schema: dataclass) -> StructType:
    # If you use dataclasses.fields, the fields must be annotated! Otherwise, they won't be seen by the fields(...)
    schema_fields = [field.default.as_field() for field in dataclasses.fields(typed_schema)]
    return StructType(fields=schema_fields)


def create_in_memory_row(*row_attributes) -> Row:
    row_dict = reduce(lambda union, next_dict: union.update(next_dict) or union, row_attributes, {})
    return Row(**row_dict)
