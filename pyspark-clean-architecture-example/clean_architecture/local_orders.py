from dataclasses import dataclass

from pyspark.sql import DataFrame

from clean_architecture.orders import EnrichedOrder


@dataclass
class OrderPoland:
    CUSTOMER_ID = 'customer_id'
    ITEM_ID = 'item_id'
    MANUFACTURER = 'manufacturer'
    DATE = 'date'
    LABEL = 'label'
    VALUE = 'value'
    UNITS = 'units'
    UNITS_TYPE_ENTITY = 'units_type_entity'

    @classmethod
    def prepare_dataframe(clz, order_to_process: DataFrame) -> DataFrame:
        prepared_order = order_to_process.withColumnsRenamed({
            clz.CUSTOMER_ID: EnrichedOrder.CUSTOMER,
            clz.DATE: EnrichedOrder.ORDER_DATE,
            clz.UNITS_TYPE_ENTITY: EnrichedOrder.UNITS_TYPE
        })

        return prepared_order.drop(clz.LABEL)

# For the sake of simplicity, let's do the same thing
@dataclass
class OrderFrance:
    CUSTOMER_ID = 'client'
    ITEM_ID = 'produit'
    MANUFACTURER = 'manufacturer'
    DATE = 'date'
    LABEL = 'label'
    VALUE = 'value'
    UNITS = 'units'
    UNITS_TYPE_ENTITY = 'units_type_entity'

    @classmethod
    def prepare_dataframe(clz, order_to_process: DataFrame) -> DataFrame:
        prepared_order = order_to_process.withColumnsRenamed({
            clz.CUSTOMER_ID: EnrichedOrder.CUSTOMER,
            clz.DATE: EnrichedOrder.ORDER_DATE,
            clz.UNITS_TYPE_ENTITY: EnrichedOrder.UNITS_TYPE,
            clz.ITEM_ID: EnrichedOrder.ITEM_ID
        })

        return prepared_order.drop(clz.LABEL)


@dataclass
class OrderGermany:
    CUSTOMER_ID = 'abnehmer'
    ITEM_ID = 'item_id'
    MANUFACTURER = 'manufacturer'
    DATE = 'date'
    LABEL = 'label'
    VALUE = 'value'
    UNITS = 'units'
    UNITS_TYPE_ENTITY = 'units_type_entity'

    @classmethod
    def prepare_dataframe(clz, order_to_process: DataFrame) -> DataFrame:
        prepared_order = order_to_process.withColumnsRenamed({
            clz.CUSTOMER_ID: EnrichedOrder.CUSTOMER,
            clz.DATE: EnrichedOrder.ORDER_DATE,
            clz.UNITS_TYPE_ENTITY: EnrichedOrder.UNITS_TYPE
        })

        return prepared_order.drop(clz.LABEL)