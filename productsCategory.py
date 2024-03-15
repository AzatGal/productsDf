from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


class ProductCategory:
    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def get_product_category_pairs(products_df, categories_df, connections_df):
        product_category_df = products_df.join(
            connections_df, products_df["product_id"] == connections_df["product_id"], "left"
        ).join(
            categories_df, connections_df["category_id"] == categories_df["category_id"], "left"
        ).select(
            products_df["product_name"], categories_df["category_name"]
        )

        '''
        нужно ли отдельно получать имена всех продуктов, у которых нет категорий?
        
        products_no_categories_df = products_df.join(
            connections_df, products_df["product_id"] == connections_df["product_id"], "left"
        ).join(
            categories_df, connections_df["category_id"] == categories_df["category_id"], "left"
        ).filter(
            categories_df["category_id"].isNull()).select(products_df["product_name"]
        )
        '''

        return product_category_df
