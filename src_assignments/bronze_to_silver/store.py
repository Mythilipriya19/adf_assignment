

from pyspark.sql.functions import split, to_date


raw_store_data_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/store/20240107_sales_store.csv', header=True, inferSchema=True)


store_data_snake_case_df = toSnakeCase(raw_store_data_df)


store_with_category_df = store_data_snake_case_df.withColumn("email_domain", split('email_address', '@')[1])\
    .withColumn("store_category", split('email_domain', '\.')[0]).drop('email_domain')


formatted_dates_df = store_with_category_df.withColumn('created_at', to_date('created_at', 'dd-MM-yyyy'))\
    .withColumn('updated_at', to_date('updated_at', 'dd-MM-yyyy'))


output_store_location = f'dbfs:/mnt/silver/sales_view/store'


write_delta_upsert(formatted_dates_df, output_store_location)


