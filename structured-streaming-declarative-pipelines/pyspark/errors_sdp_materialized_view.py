from pyspark import pipelines, Row
from pyspark.sql import SparkSession

@pipelines.materialized_view(format='delta')
def error_in_memory_numbers():
    spark = SparkSession.active()
    old_letters = spark.read.table('in_memory_numbers_temporary_view')

    new_letters = spark.createDataFrame(
        data=[
            Row(id=1, letter='A'), Row(id=2, letter='B'), Row(id=3, letter='C')
        ]
    )

    return old_letters.unionByName(new_letters, allowMissingColumns=False)

@pipelines.temporary_view()
def in_memory_numbers_temporary_view():
    spark = SparkSession.active()
    return spark.createDataFrame(
        data=[
            Row(id=4, letter=44)
        ]
    )

@pipelines.materialized_view(format='delta')
def error_text_letters_materialized_view():
    spark = SparkSession.active()
    return spark.read.text('/opt/spark/files')
