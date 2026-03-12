from pyspark.sql import functions as F, SparkSession

spark = (SparkSession.builder.remote('sc://localhost:15002')
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
         .getOrCreate())

raw_stream = (
    spark.readStream
    .format('rate-micro-batch')
    .option('rowsPerBatch', 10)
    .option('numPartitions', 5)
    .load()
)
rate_writer = (raw_stream
                .withColumn('ingestion_time', F.current_timestamp())
                .select(F.to_json(F.struct('*')).alias('value')))

query = (
    rate_writer.writeStream
        .trigger(processingTime='2 seconds')
        .option('checkpointLocation', './data/checkpoints/delta_producer_tmp')
        .start(
            path='/opt/spark/data/rate_delta_for_input',
            format='delta'
        )
)

query.awaitTermination()
