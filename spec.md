PSI Data Engineering Challenge: E-Commerce Pipeline Spec
1. Project Goal
Build a PySpark ETL pipeline to transform raw, messy e-commerce CSV data into a clean, enriched analytical layer for business intelligence.

2. Technical Stack & Constraints
Stack: PySpark 3.4+, Python 3.10.

Execution: Local SparkSession (no cluster/Databricks assumed).

Native Logic: Use pyspark.sql.functions (F) and pyspark.sql.Window. No Python UDFs and no .toPandas() inside the core pipeline.

Idempotency: All writes must use mode('overwrite').

3. Data Schema & Quality Issues
Tables
orders.csv: order_id, customer_id, order_date, status, total_amount, discount_pct.

order_items.csv: item_id, order_id, product_id, quantity, unit_price, category.

customers.csv: customer_id, signup_date, country, customer_tier.

returns.csv: return_id, order_id, return_date, reason, refund_amount.

Known Issues (Must be handled explicitly)
Duplicates: ~8% exact duplicates across all tables.

Date formats: Mix of YYYY-MM-DD and DD/MM/YYYY.

NULLs: NULL customer_id and total_amount in orders.

Negatives: Negative total_amount in orders (flag, don't drop).

Orphans: order_items referencing non-existent order_id.

Casing: Inconsistent casing in customer_tier.

4. Required Tasks
Task 01: Ingestion
Define explicit StructType schemas for all tables.

Redirect rows that fail casting into a separate rejected DataFrame and log them.

Task 02: Cleaning
Remove duplicates.

Normalize dates to ISO YYYY-MM-DD.

Lowercase customer_tier.

Drop rows where order_id or customer_id is NULL.

Add is_negative_amount boolean flag for total_amount < 0.

Task 03: Joins & Enrichment
Join orders, customers, and order_items.

Use an anti-join to isolate orphaned order_items to a separate output.

Calculate net_amount = total_amount * (1 - discount_pct / 100).

Task 04: Analytical Window Functions
Customer Ranking: Rank by lifetime net spend within each country.

Rolling Metrics: 7-day rolling order count per customer.

Revenue Share: Each category's % share of total revenue per calendar month.

Task 05: Return Analysis
Join returns to the enriched orders.

Compute return rates by category and customer_tier.

Flag refund_exceeds_order where refund_amount > net_amount.

Task 06: Output
Final data: Write to Parquet, partitioned by year and month.

Aggregates: Write top 10 refund customers to CSV.

5. Bonus Goals (Priority)
B1: pytest unit tests for cleaning logic.

B2: Use broadcast hint for the customers table join.

B3: Add a Data Quality Gate (exception if customer_id is NULL or net_amount < 0).