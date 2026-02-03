🛒 Retail Analytics Data Pipeline

End-to-End Batch ETL using Medallion Architecture (Bronze → Silver → Gold)

Overview

This project implements a production-style batch data pipeline using PySpark and the Medallion Architecture pattern.
The goal is to simulate how raw business data flows through real data engineering systems — from raw ingestion to analytics-ready tables used for reporting and dashboards.

Architecture
Raw CSV
   ↓
Bronze → Silver → Gold → Analytics

Tech Stack

PySpark

Parquet

Linux / WSL

Git & GitHub

Spark SQL

Power BI

Repository Structure
RetailDataPipeline/
├── analytics/
│   ├── retail_analytics.sql
│   └── run_analytics.py
│
├── bronze/
│   └── bronze.py
│
├── silver/
│   └── silver.py
│
├── gold/
│   ├── dim_customer.py
│   ├── dim_product.py
│   ├── dim_date.py
│   └── fact_sales.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── run_pipeline.sh

Bronze Layer

Stores raw data exactly as received.
No transformations. Written as Parquet.

Output: data/bronze/online_retail_raw

Silver Layer

Creates a clean and trusted dataset.

Transformations

Parse InvoiceDate

Enforce data types

Remove invalid records

Business-key deduplication

Output: data/silver/retail_clean

Gold Layer

Analytics-ready star schema.

Dimensions

dim_customer

dim_product

dim_date

Fact

fact_sales

Analytics

Spark SQL queries on top of Gold tables.

Run:

spark-submit analytics/run_analytics.py

Pipeline Orchestration

Run full pipeline:

./run_pipeline.sh

What I Learned

Medallion Architecture

Data validation & modeling

Fact/dimension design

Spark DataFrames + SQL

Batch orchestration
