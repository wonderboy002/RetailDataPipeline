from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("fact_sales").getOrCreate()

df = spark.read.parquet("data/silver/retail_clean")

fact_sales = (
    df.select(
        col("Invoice").alias("invoice_id"),
        col("StockCode").alias("product_id"),
        col("Customer ID").alias("customer_id"),
        date_format(to_date("InvoiceDate"), "yyyyMMdd").cast("int").alias("date_id"),
        col("Price").alias("unit_price"),
        col("Quantity").alias("quantity"),
        round(col("Price") * col("Quantity"),2).alias("sales_amount"),
        
    )
)

fact_sales.write.mode("overwrite").parquet("data/gold/fact_sales")
