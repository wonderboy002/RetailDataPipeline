#!/bin/bash
echo "Starting Retail Medallion Pipeline..."

spark-submit src/bronze/ingest_raw.py
if [ $? -ne 0 ]; then
  echo "Bronze layer failed"
  exit 1
fi

spark-submit src/silver/clean_retail.py
if [ $? -ne 0 ]; then
  echo "Silver layer failed"
  exit 1
fi

spark-submit src/gold/dim_product.py
if [ $? -ne 0 ]; then
  echo "dim_product failed"
  exit 1
fi

spark-submit src/gold/dim_customer.py
if [ $? -ne 0 ]; then
  echo "dim_customer failed"
  exit 1
fi

spark-submit src/gold/dim_date.py
if [ $? -ne 0 ]; then
  echo "dim_date failed"
  exit 1
fi

spark-submit src/gold/fact_sales.py
if [ $? -ne 0 ]; then
  echo "fact_sales failed"
  exit
fi