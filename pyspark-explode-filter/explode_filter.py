from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("explode-then-filter")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config('spark.sql.adaptive.enabled', False)
    .getOrCreate()
)

orders = spark.read.parquet('data')

exploded = orders.select("order_id", "customer_id", "order_date", F.explode("items").alias("item"))
cancelled_items = exploded.filter(F.col("item.status") == "cancelled")


print("=" * 80)
print("EXECUTION PLAN - explode -> filter")
print("=" * 80)
#cancelled_items.explain(mode='extended')
#cancelled_items.explain(mode='codegen')


cancelled_items.write.format("noop").mode("append").save()

while True:
    pass
