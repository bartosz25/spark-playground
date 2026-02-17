from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import DATA_DIR, CHECKPOINT_DIR

if __name__ == '__main__':
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master('local[*]')
                                                   .config('spark.sql.extensions',
                                                           'io.delta.sql.DeltaSparkSessionExtension')
                                                   .config('spark.sql.catalog.spark_catalog',
                                                           'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    offsets = (spark_session.read.text(f'{CHECKPOINT_DIR}/offsets')
               .withColumn('micro_batch_version',
                           F.element_at(F.split(F.input_file_name(), "/"), -1).cast('int'))
               .withColumn('progress_source',
                           F.when(F.col('value').contains('reservoirId'), 'Delta Lake')
                           .when(F.col('value').startswith('{"visits'), 'Apache Kafka')
                           .otherwise(None))
               .filter('progress_source IS NOT NULL')
               )

    offsets.show(truncate=False)
