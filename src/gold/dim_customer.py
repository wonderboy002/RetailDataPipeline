from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

spark=SparkSession.builder.appName("dim_product").getOrCreate()
sc=spark.sparkContext

df = spark.read.parquet("data/silver/retail_clean")
dim_customer=df.select(col("Customer ID").alias("customer_id"),col("Country").alias("country"))
dim_customer=dim_customer.dropDuplicates(["customer_id","country"])
dim_customer.write.mode("overwrite").parquet("data/gold/dim_customer")