from dataclasses import dataclass

from pyspark.sql import DataFrame

from clean_architecture.referential import ProductReferential


@dataclass
class EnrichedOrder:
    CUSTOMER = 'customer'
    ITEM_ID = ProductReferential.ITEM_ID
    MANUFACTURER = ProductReferential.MANUFACTURER
    MODEL = ProductReferential.MODEL
    ORDER_DATE = 'order_date'
    VALUE = 'value'
    UNITS = 'units'
    UNITS_TYPE = 'units_type'

    ALL_COLUMNS = [CUSTOMER, ITEM_ID, MANUFACTURER, MODEL, ORDER_DATE, VALUE, UNITS, UNITS_TYPE]

    @classmethod
    def prepare_enriched_order(clz, orders_poland: DataFrame, orders_france: DataFrame, orders_germany: DataFrame,
                               referential_product: DataFrame) -> DataFrame:
        unified_orders = (orders_poland.unionByName(orders_germany, allowMissingColumns=False)
                          .unionByName(orders_france, allowMissingColumns=False))

        enriched_orders = unified_orders.join(referential_product,
                                              on=[ProductReferential.ITEM_ID, ProductReferential.MANUFACTURER],
                                              how='left')
        return enriched_orders.select(clz.ALL_COLUMNS)
