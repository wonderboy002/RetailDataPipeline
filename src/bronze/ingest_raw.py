from pyspark.sql import SparkSession 
spark=SparkSession.builder.appName("BronzeIngestion").getOrCreate()
sc=spark.sparkContext

df = spark.read.csv("data/source/online_retail_II.csv",header=True,inferSchema=True,sep=",")

df.write.mode("overwrite").parquet("data/bronze/online_retail_raw")
spark.stop()