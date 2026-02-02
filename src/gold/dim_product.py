from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

spark=SparkSession.builder.appName("dim_product").getOrCreate()
sc=spark.sparkContext

df = spark.read.parquet("data/silver/retail_clean")
dim_product=df.select(col("StockCode").alias("product_id"),col("Description").alias("product_description"))
dim_product=dim_product.dropDuplicates(["product_id","product_description"])
dim_product.write.mode("overwrite").parquet("data/gold/dim_product")