from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import DATA_DIR

if __name__ == '__main__':
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master('local[*]')
                                                   .config('spark.sql.extensions',
                                                           'io.delta.sql.DeltaSparkSessionExtension')
                                                   .config('spark.sql.catalog.spark_catalog',
                                                           'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    kafka_input_stream = (spark_session.read
                          .option('kafka.bootstrap.servers', 'localhost:9094')
                          .option('subscribe', 'observability')
                          .option('startingOffsets', 'EARLIEST')
                          .format('kafka').load())
    kafka_input_stream.select(F.col('value').cast('string')).show(truncate=False)
