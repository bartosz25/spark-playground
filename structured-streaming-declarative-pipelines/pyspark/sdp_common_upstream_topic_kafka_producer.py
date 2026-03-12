from pyspark.sql import functions as F, SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()

raw_stream = (
    spark.readStream
    .format('rate-micro-batch')
    .option('rowsPerBatch', 10)
    .option('numPartitions', 5)
    .load()
)
kafka_writer = (raw_stream
                .withColumn('ingestion_time', F.current_timestamp())
                .select(F.to_json(F.struct('*')).alias('value')))

query = (
    kafka_writer.writeStream
        .trigger(processingTime='2 seconds')
        .format('kafka')
        .option('kafka.bootstrap.servers', 'kafka:9092')
        .option('topic', 'rate-data')
        .option('checkpointLocation', './data/checkpoints/kafka_producer_tmp')
        .start()
)

query.awaitTermination()
