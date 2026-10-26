from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("filter-inside-array")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config('spark.sql.adaptive.enabled', False)
    .getOrCreate()
)

orders = spark.read.parquet('data')
cancelled_items = orders.select(
    "order_id", "customer_id", "order_date",
    F.explode(F.filter("items", lambda item: item.status == "cancelled")).alias("item"),
)

print("=" * 80)
print("EXECUTION PLAN - filter() higher-order function on the array")
print("=" * 80)
#cancelled_items.explain(mode='extended')
#cancelled_items.explain(mode='codegen')


cancelled_items.write.format("noop").mode("append").save()

while True:
    pass
