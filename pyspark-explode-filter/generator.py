from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def generate_orders(spark: SparkSession, orders: int, items_per_order: int):
    return (
        spark.range(orders)
        .withColumnRenamed("id", "order_id")
        .withColumn("customer_id", F.pmod(F.hash("order_id"), F.lit(20_000)))
        .withColumn("order_date", F.date_add(F.lit("2026-01-01").cast("date"), F.pmod("order_id", F.lit(365)).cast("int")))
        .withColumn(
            "items",
            F.transform(
                F.sequence(F.lit(1), F.lit(items_per_order)),
                lambda i: F.struct(
                    F.concat(F.lit("SKU-"), i.cast("string")).alias("sku"),
                    F.round(F.pmod(F.hash(F.col("order_id"), i), F.lit(10_000)) / 100, 2).alias("price"),
                    F.when(F.pmod(F.hash(F.col("order_id"), i, F.lit("status")), F.lit(20)) == 0, "cancelled")
                    .otherwise("delivered")
                    .alias("status"),
                ),
            ),
        )
    )


spark_session = (
    SparkSession.builder.appName("explode-then-filter")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

generate_orders(
    spark=spark_session, orders=1_000_000, items_per_order=50
).write.mode("overwrite").parquet('./data')
for _ in range(0, 15):
    generate_orders(
        spark=spark_session, orders=1_000_000, items_per_order=50
    ).write.mode("append").parquet('./data')
