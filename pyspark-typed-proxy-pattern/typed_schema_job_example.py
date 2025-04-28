from pyspark.sql import SparkSession

from schemas_declarations import PeopleSchema
from typed_schemas import create_spark_schema_for_typed_schema, create_in_memory_row
from filters import get_people_older_than
from mappers import add_birth_year

spark = SparkSession.Builder().config("spark.ui.showConsoleProgress", "false").getOrCreate()

rows = [(create_in_memory_row(
    PeopleSchema.id.for_row(1), PeopleSchema.name.for_row('name_1'), PeopleSchema.age.for_row(20),
    PeopleSchema.address.for_row([
        PeopleSchema.address.city.for_row([
            PeopleSchema.address.city.city_name.for_row('Some city'),
            PeopleSchema.address.city.zip_code.for_row('00000')
        ]), PeopleSchema.address.street_name.for_row('Some street')
    ])
)), (create_in_memory_row(
    PeopleSchema.id.for_row(2), PeopleSchema.name.for_row('name_2'), PeopleSchema.age.for_row(30),
    PeopleSchema.address.for_row([
        PeopleSchema.address.city.for_row([
            PeopleSchema.address.city.city_name.for_row('Some city 2'),
            PeopleSchema.address.city.zip_code.for_row('00001')
        ]), PeopleSchema.address.street_name.for_row('Some street 2')
    ])
))]

people_dataset = spark.createDataFrame(rows, create_spark_schema_for_typed_schema(PeopleSchema))

people_dataset.show(truncate=False)
people_dataset.printSchema()

older_than_18 = get_people_older_than(people_dataset, 18)
older_than_18.show(truncate=False)

older_than_18_with_birth_year = add_birth_year(older_than_18)
older_than_18_with_birth_year.show(truncate=False)
