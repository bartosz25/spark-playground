from dataclasses import dataclass

from pyspark.sql.types import IntegerType, StringType, StructType

from typed_schemas import SchemaAttribute, AddressSchemaAttribute


@dataclass(frozen=True)
class PeopleSchema:
    id: SchemaAttribute = SchemaAttribute(name='id', type=IntegerType())
    name: SchemaAttribute = SchemaAttribute(name='name', type=StringType())
    age: SchemaAttribute = SchemaAttribute(name='age', type=IntegerType())
    address: AddressSchemaAttribute = AddressSchemaAttribute(name='address', type=StructType())
