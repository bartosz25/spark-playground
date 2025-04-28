from pyspark.sql import DataFrame, functions as F

from schemas_declarations import PeopleSchema


def get_people_older_than(people_df: DataFrame, age_limit: int) -> DataFrame:
    return people_df.filter(PeopleSchema.age.as_column() > age_limit)


def get_people_older_than_untyped(people_df: DataFrame, age_limit: int) -> DataFrame:
    return people_df.filter(F.col('age') > age_limit)
