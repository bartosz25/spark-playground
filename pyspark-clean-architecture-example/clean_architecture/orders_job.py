import shutil

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame

from clean_architecture.local_orders import OrderPoland, OrderFrance, OrderGermany
from clean_architecture.orders import EnrichedOrder
from clean_architecture.referential import ProductReferential
from clean_architecture.setup_referential_and_orders import setup_data


def generate_enriched_orders(raw_poland: DataFrame, raw_france: DataFrame,
                             raw_germany: DataFrame, raw_referential_data: DataFrame) -> DataFrame:
    orders_poland = OrderPoland.prepare_dataframe(raw_poland)
    orders_france = OrderFrance.prepare_dataframe(raw_france)
    orders_germany = OrderGermany.prepare_dataframe(raw_germany)
    referential_data = ProductReferential.prepare_dataframe(raw_referential_data)

    enriched_orders = EnrichedOrder.prepare_enriched_order(
        orders_poland=orders_poland, orders_france=orders_france, orders_germany=orders_germany,
        referential_product=referential_data
    )

    return enriched_orders



if __name__ == '__main__':
    shutil.rmtree("../data/catalog", ignore_errors=True)
    spark = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                            .config("spark.sql.extensions",
                                                    "io.delta.sql.DeltaSparkSessionExtension")
                                            .config("spark.sql.catalog.spark_catalog",
                                                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                            .config("spark.sql.warehouse.dir", "../data/catalog")
                                            ).getOrCreate())
    setup_data(spark)
    enriched_orders = generate_enriched_orders(
        raw_poland=spark.read.csv('../data/poland_orders', header=True, inferSchema=True),
        raw_france=spark.read.csv('../data/france_orders', header=True, inferSchema=True),
        raw_germany=spark.read.csv('../data/germany_orders', header=True, inferSchema=True),
        raw_referential_data=spark.read.table('product_referential')
    )
    enriched_orders.write.format('delta').mode('overwrite').saveAsTable('enriched_orders')

    spark.table('enriched_orders').show(truncate=False)