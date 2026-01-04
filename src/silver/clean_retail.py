from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("SilverCleaning").getOrCreate()
sc=spark.sparkContext

df_main=spark.read.parquet("data/bronze/online_retail_raw")
df_main=df_main.withColumn("InvoiceDate",to_timestamp("InvoiceDate","M/d/yy H:mm"))
df_main=df_main.withColumn("Customer ID",col("Customer ID").cast("string"))
df_main.printSchema()

df_main=df_main.filter((col("Price")>0) & (col("Quantity")>0))
df_main=df_main.filter(
    (col("Invoice").isNotNull()) & 
    (col("StockCode").isNotNull()) & 
    (col("Customer ID").isNotNull()) & 
    (col("Quantity").isNotNull()) & 
    (col("Price").isNotNull()) & 
    (col("InvoiceDate").isNotNull())
)

null_count = df_main.filter(
    (col("Invoice").isNull()) |
    (col("StockCode").isNull()) |
    (col("Customer ID").isNull()) |
    (col("Quantity").isNull()) |
    (col("Price").isNull()) |
    (col("InvoiceDate").isNull())
).count()

print("Number of Null Rows",null_count)
before=df_main.count()
print("before Duplicates Removal: ",before)
df_main=df_main.dropDuplicates(["Invoice","StockCode","InvoiceDate","Customer ID"])

after=df_main.count()
print("after Duplicates Removal",after)

df_main.write.mode("overwrite").parquet("data/silver/retail_clean")

spark.stop()