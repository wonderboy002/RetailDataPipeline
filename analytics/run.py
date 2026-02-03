from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("analytics").getOrCreate()

dim_customer=spark.read.parquet("data/gold/dim_customer")
dim_product=spark.read.parquet("data/gold/dim_product")                    
dim_date=spark.read.parquet("data/gold/dim_date")
fact_sales=spark.read.parquet("data/gold/fact_sales")

dim_customer.createOrReplaceTempView("dim_customer")
dim_product.createOrReplaceTempView("dim_product")
dim_date.createOrReplaceTempView("dim_date")
fact_sales.createOrReplaceTempView("fact_sales")


with open("analytics/queries.sql","r") as fobj:
    queries=fobj.readlines()
    for query in queries:
        q=query.strip()
        result=spark.sql(q)
        result.show()    
