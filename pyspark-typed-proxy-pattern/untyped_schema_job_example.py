from pyspark import Row
from pyspark.sql import SparkSession

from filters import get_people_older_than_untyped
from mappers import add_birth_year_untyped

spark = SparkSession.Builder().config("spark.ui.showConsoleProgress", "false").getOrCreate()

rows = [
    Row(id=1, name='name_1', age=20, address={
        'city': {'city_name': 'Some city', 'zip_code': '00000'}, 'street_name': 'Some street'
    }), Row(id=2, name='name_2', age=30, address={
        'city': {'city_name': 'Some city 2', 'zip_code': '00001'}, 'street_name': 'Some street 2'
    })
]

people_dataset = spark.createDataFrame(rows, 'id INT, name STRING, age INT, address STRUCT<street_name STRING, city STRUCT<city_name STRING, zip_code STRING>>')
older_than_18 = get_people_older_than_untyped(people_dataset, 18)
older_than_18_with_birth_year = add_birth_year_untyped(older_than_18)
older_than_18_with_birth_year.show(truncate=False)
