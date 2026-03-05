from pyspark import pipelines
from pyspark.sql import SparkSession, functions as F

pipelines.create_sink('numbers', format='kafka', options={
    'kafka.bootstrap.servers': 'kafka:9092', # the code will be running on Docker, so use container's host
    'topic': 'numbers'
})


@pipelines.append_flow(target='numbers')
def rate_source():
    spark = SparkSession.active()
    return (spark.readStream.text(path='/opt/spark/files').select('value'))
