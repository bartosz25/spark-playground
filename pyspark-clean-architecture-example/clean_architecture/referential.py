from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class ProductReferential:
    ITEM_ID = 'item_id'
    MANUFACTURER = 'manufacturer'
    MODEL = 'model'

    ALL_COLUMNS = [ITEM_ID, MANUFACTURER, MODEL]

    @classmethod
    def prepare_dataframe(clz, product_referential: DataFrame) -> DataFrame:
        return product_referential.select(clz.ALL_COLUMNS)

