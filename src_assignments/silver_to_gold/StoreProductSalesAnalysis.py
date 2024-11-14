from pyspark.sql.functions import col

product_data_path = 'dbfs:/mnt/silver/sales_view/product'
store_data_path = 'dbfs:/mnt/silver/sales_view/store'


product_df = read_delta_file(product_data_path)
store_df = read_delta_file(store_data_path)


merged_product_store_df = product_df.join(store_df, product_df.store_id == store_df.store_id, "inner")

product_store_with_details_df = merged_product_store_df.select(
    store_df.store_id,
    "store_name", "location", "manager_name",
    "product_id", "product_name", "product_code",
    "description", "category_id", "price",
    "stock_quantity", "supplier_id",
    product_df.created_at.alias("product_created_at"),
    product_df.updated_at.alias("product_updated_at"),
    "image_url", "weight", "expiry_date", "is_active",
    "tax_rate"
)


customer_sales_data_path = "dbfs:/mnt/silver/sales_view/customer_sales"
customer_sales_df = read_delta_file(customer_sales_data_path)


merged_product_customer_sales_df = product_store_with_details_df.join(
    customer_sales_df, product_store_with_details_df.product_id == customer_sales_df.product_id, "inner"
)


final_sales_analysis_df = merged_product_customer_sales_df.select(
    "OrderDate", "Category", "City", "CustomerID", "OrderID",
    product_store_with_details_df.product_id.alias('ProductID'),
    "Profit", "Region", "Sales", "Segment", "ShipDate",
    "ShipMode", "latitude", "longitude",
    "store_name", "location", "manager_name",
    "product_name", "price", "stock_quantity",
    "image_url"
)


final_output_path = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysis"


write_delta_upsert(final_sales_analysis_df, final_output_path)
