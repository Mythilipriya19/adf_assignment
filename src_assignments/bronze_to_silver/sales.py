
raw_sales_data_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/sales/20240107_sales_data.csv', header=True, inferSchema=True)



sales_data_snake_case_df = toSnakeCase(raw_sales_data_df)




sales_output_location = 'dbfs:/mnt/silver/sales_view/customer_sales'


write_delta_upsert(sales_data_snake_case_df, sales_output_location)


# code

