from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import DATA_DIR
from progress_tracking_listener import ProgressTrackingListener

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                    .master("local[*]")
                                                    .config('spark.sql.shuffle.partitions', 2)
                                                    .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1']
                                                   ).getOrCreate())
    kafka_input_stream = (spark_session.readStream
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('subscribe', 'visits')
                         .option('startingOffsets', 'LATEST')
                         .format('kafka').load())
    visits_from_kafka: DataFrame = (kafka_input_stream
                                    .select(F.from_json(F.col('value').cast('string'), 'user_id INT, visit_time TIMESTAMP, page STRING')
                                            .alias('value'))
                                    .selectExpr('value.*'))
    visits_from_delta = spark_session.readStream.format('delta').load(DATA_DIR)

    visits = visits_from_delta.unionByName(visits_from_kafka)

    spark_session.streams.addListener(ProgressTrackingListener(
        job_name='read_visits', output_topic='observability', broker_host_port='localhost:9094'
    ))
    write_query = visits.writeStream.format("console").option('truncate', False).start()

    write_query.awaitTermination()
