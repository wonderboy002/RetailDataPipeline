🛒 Retail Analytics Data Pipeline

End-to-End Batch ETL using Medallion Architecture (Bronze → Silver → Gold)

Overview

This project implements a production-style batch data pipeline using PySpark and the Medallion Architecture pattern.
The goal is to simulate how raw business data flows through real data engineering systems — from raw ingestion to analytics-ready tables used for reporting and dashboards.

The pipeline processes the Online Retail II dataset and produces a star schema that can be directly consumed by BI tools or queried using Spark SQL.

Architecture:
Raw CSV
   ↓
Bronze Layer  → Raw, replayable data
   ↓
Silver Layer  → Cleaned, validated, deduplicated data
   ↓
Gold Layer    → Star Schema (Fact + Dimensions)
   ↓
Spark SQL / Power BI

Tech Stack:
PySpark
Parquet
Linux / WSL
Git
Spark SQL

Project Structure
RetailDataPipeline/
│
├── bronze/
│   └── bronze.py
├── silver/
│   └── silver.py
├── gold/
│   ├── dim_customer.py
│   ├── dim_product.py
│   ├── dim_date.py
│   └── fact_sales.py
├── analytics/
│   ├── retail_analytics.sql
│   └── run_analytics.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── run_pipeline.sh

Bronze Layer

Purpose: Store raw data exactly as received.

Reads the original Online Retail II CSV

No transformations

Writes data as Parquet

Acts as a replayable and auditable source

Output:
data/bronze/online_retail_raw

Silver Layer

Purpose: Create a trusted and clean dataset.

Transformations applied:

Parsed InvoiceDate using a fixed format

Enforced data types

Removed invalid records:

Quantity ≤ 0

Price ≤ 0

NULLs in business keys

Deduplicated using the business key:

(Invoice, StockCode, InvoiceDate, CustomerID)


Output:
data/silver/retail_clean

Gold Layer

Purpose: Analytics-ready star schema.

Dimensions

dim_customer → customer_id, country

dim_product → product_id, product_description

dim_date → date_id, full_date, year, month, day, quarter, week_of_year

Fact

fact_sales

invoice_id

date_id

customer_id

product_id

quantity

unit_price

sales_amount

This structure allows easy slicing and aggregation in BI tools.

Analytics (Spark SQL)

Gold tables are registered as temp views and queried using Spark SQL.

Examples of analytics:

Revenue by customer

Revenue by country

High value transactions

Customers above average spend

All queries are stored in:

analytics/retail_analytics.sql


Executed via:

spark-submit analytics/run_analytics.py

Pipeline Orchestration

The full pipeline runs automatically using:

./run_pipeline.sh


This executes:

Bronze ingestion

Silver transformations

Gold dimensions

Gold fact

Analytics-ready outputs

What I Learned:

How Medallion Architecture works in real pipelines

Data quality validation and business-key deduplication

Fact and dimension modeling (star schema)

Using Spark DataFrames and Spark SQL together

Batch pipeline orchestration using shell scripting

Future Improvements:

Incremental loads instead of full refresh

Partitioning Gold fact tables

Logging and monitoring

Cloud deployment (S3 / Databricks)