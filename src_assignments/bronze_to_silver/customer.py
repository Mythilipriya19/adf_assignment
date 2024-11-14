from pyspark.sql.functions import split, when, col, to_date


raw_customer_data_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/customer/20240107_sales_customer.csv', header=True, inferSchema=True)


snake_case_customer_df = toSnakeCase(raw_customer_data_df)


customer_names_split_df = snake_case_customer_df.withColumn('first_name', split(snake_case_customer_df.name, " ")[0])\
    .withColumn('last_name', split(snake_case_customer_df.name, " ")[1]).drop(snake_case_customer_df.name)


email_domain_extracted_df = customer_names_split_df.withColumn("temp_domain", split(customer_names_split_df.email_id, "@")[1]).drop(customer_names_split_df.email_id)
email_domain_extracted_df = email_domain_extracted_df.withColumn('domain', split(email_domain_extracted_df.temp_domain, '\.')[0]).drop(email_domain_extracted_df.temp_domain)


gender_converted_df = email_domain_extracted_df.withColumn('gender', when(col('gender') == 'male', 'M')\
    .otherwise('F'))


joining_date_split_df = gender_converted_df.withColumn('date', split(col('joining_date'), " ")[0])\
    .withColumn('time', split(col('joining_date'), ' ')[1]).drop('joining_date')


date_converted_df = joining_date_split_df.withColumn('date', to_date(col('date'), 'dd-MM-yyyy'))


expenditure_category_df = date_converted_df.withColumn('expenditure_status', when(col('spent') < 200, 'MINIMUM').otherwise('MAXIMUM'))


output_location = 'dbfs:/mnt/silver/sales_view/customer'


expenditure_category_df.write.format('delta').save(output_location)
