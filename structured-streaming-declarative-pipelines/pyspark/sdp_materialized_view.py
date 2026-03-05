from pyspark import pipelines, Row
from pyspark.sql import SparkSession

@pipelines.materialized_view(format='delta')
def in_memory_numbers():
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
            Row(id=4, letter='D'), Row(id=5, letter='E'), Row(id=6, letter='F')
        ]
    )

@pipelines.materialized_view(format='delta')
def text_letters_materialized_view():
    spark = SparkSession.active()
    return spark.read.text('/opt/spark/files')


"""
@pipelines.materialized_view(format='delta')
def text_letters_materialized_view_streaming():
    spark = SparkSession.active()
    return spark.readStream.text('/opt/spark/files')
"""
