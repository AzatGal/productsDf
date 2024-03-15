from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from productsCategory import *


spark = SparkSession.builder.getOrCreate()

products_df = spark.createDataFrame([
    (1, 'Продукт 1'),
    (2, 'Продукт 2'),
    (3, 'Продукт 3'),
    (4, 'Продукт 4')
], ['product_id', 'product_name'])

categories_df = spark.createDataFrame([
    (1, 'Категория 1'),
    (2, 'Категория 2'),
    (3, 'Категория 3')
], ['category_id', 'category_name'])

product_category_df = spark.createDataFrame([
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 3)
], ['product_id', 'category_id'])


result_df = ProductCategory.get_product_category_pairs(products_df, categories_df, product_category_df)


result_df.show()
