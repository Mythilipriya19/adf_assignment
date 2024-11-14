

from pyspark.sql.functions import when, col




raw_product_data_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/product/20240107_sales_product.csv', header=True, inferSchema=True)




snake_case_product_df = toSnakeCase(raw_product_data_df)




product_with_sub_category_df = snake_case_product_df.withColumn("sub_category", when(col('category_id') == 1, "phone")\
    .when(col('category_id') == 2, "laptop")\
    .when(col('category_id') == 3, "playstation")\
    .when(col('category_id') == 4, "e-device"))




output_location = 'dbfs:/mnt/silver/sales_view/product'


write_delta_upsert(product_with_sub_category_df, output_location)

# command
