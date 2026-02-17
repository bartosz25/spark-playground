import datetime

from delta import configure_spark_with_delta_pip
from pyspark import Row
from pyspark.sql import SparkSession, functions as F

from config import DATA_DIR

if __name__ == '__main__':
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master('local[*]')
                                                   .config('spark.sql.extensions',
                                                           'io.delta.sql.DeltaSparkSessionExtension')
                                                   .config('spark.sql.catalog.spark_catalog',
                                                           'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1']
                                                   ).getOrCreate())

    visits = spark_session.createDataFrame(data=[
        Row(user_id=1, visit_time=datetime.datetime.now(tz=datetime.timezone.utc), page='page_1.html'),
        Row(user_id=1, visit_time=datetime.datetime.now(tz=datetime.timezone.utc), page='page_2.html'),
        Row(user_id=2, visit_time=datetime.datetime.now(tz=datetime.timezone.utc), page='page_3.html'),
        Row(user_id=3, visit_time=datetime.datetime.now(tz=datetime.timezone.utc), page='page_4.html'),
    ])

    visits.write.mode('append').format('delta').save(DATA_DIR)

    (visits.withColumn('value', F.to_json(F.struct('*'))).select('value')
     .write.format('kafka')
     .option('kafka.bootstrap.servers', 'localhost:9094')
     .option('topic', 'visits').save())
