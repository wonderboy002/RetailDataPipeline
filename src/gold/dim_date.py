from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

spark=SparkSession.builder.appName("dim_product").getOrCreate()
sc=spark.sparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("dim_date").getOrCreate()

df = spark.read.parquet("data/silver/retail_clean")

dim_date = (
    df
    .select(to_date("InvoiceDate").alias("full_date"))
    .distinct()
    .withColumn("date_id", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("week_of_year", weekofyear(col("full_date")))
)

dim_date.write.mode("overwrite").parquet("data/gold/dim_date")
