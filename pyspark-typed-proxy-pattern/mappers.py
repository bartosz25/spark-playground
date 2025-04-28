from pyspark.sql import DataFrame, functions as F

from schemas_declarations import PeopleSchema


def add_birth_year(people_df: DataFrame) -> DataFrame:
    return people_df.withColumn('birth_year', F.year(F.current_date()) - PeopleSchema.age.as_column())


def add_birth_year_untyped(people_df: DataFrame) -> DataFrame:
    return people_df.withColumn('birth_year', F.year(F.current_date()) - F.col('age'))