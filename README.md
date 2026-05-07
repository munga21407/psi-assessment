# PSI Data Engineering Assessment

A PySpark ETL pipeline that ingests raw e-commerce CSVs, cleans and enriches the data, computes analytical aggregations via Window functions, and writes the results to partitioned Parquet and summary CSV outputs.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Requirements](#requirements)
3. [Setup](#setup)
4. [Running the Pipeline](#running-the-pipeline)
5. [Pipeline Tasks](#pipeline-tasks)
6. [Data Quality](#data-quality)
7. [Outputs](#outputs)
8. [Known Issues & Decisions](#known-issues--decisions)

---

## Project Structure

```
psi-assessment/
├── data/                          # Input CSVs (not committed)
│   ├── orders.csv
│   ├── order_items.csv
│   ├── customers.csv
│   └── returns.csv
├── output/                        # Generated outputs (not committed)
│   ├── enriched_orders/           # Parquet, partitioned by year/month
│   └── top10_refund_customers/    # Summary CSV
├── src/
│   ├── schemas.py                 # PySpark StructType definitions
│   ├── pipeline.py                # All ETL logic + entry point
│   └── tests.py                   # pytest unit tests (bonus)
├── hadoop/
│   └── bin/
│       ├── winutils.exe           # Required on Windows only
│       └── hadoop.dll             # Required on Windows only
├── spec.md                        # Challenge requirements
├── pyproject.toml
└── uv.lock
```

---

## Requirements

| Dependency | Version |
|---|---|
| Python | 3.10+ |
| PySpark | 3.5+ |
| Java | **17 LTS** (required — see note below) |
| pandas | 2.3+ |
| numpy | 2.2+ |
| faker | 40+ |
| pytest | 9+ |

> **Java version matters.** PySpark's bundled Hadoop calls `javax.security.auth.Subject.getSubject()`, which was removed in Java 21. Use Java 17 LTS.
> Install via: `winget install Microsoft.OpenJDK.17`

---

## Setup

### 1. Clone / extract the project

```bash
cd psi-assessment
```

### 2. Create the virtual environment with Python 3.10

```bash
uv venv --python 3.10
uv sync
```

### 3. Windows only — Hadoop native binaries

Spark on Windows requires `winutils.exe` and `hadoop.dll` to write files. They are included in `hadoop/bin/`. If missing, download Hadoop 3.3.6 binaries from [cdarlint/winutils](https://github.com/cdarlint/winutils).

### 4. Set environment variables

**Windows (Git Bash / PowerShell):**

```bash
export JAVA_HOME="/c/Program Files/Microsoft/jdk-17.0.19.10-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
```

Or set them permanently in System Properties > Environment Variables.

---

## Running the Pipeline

From the `psi-assessment/` root:

```bash
python -c "
import os
os.environ['HADOOP_HOME'] = r'C:\Users\<you>\psi-assessment\hadoop'
os.environ['PATH'] = r'C:\Users\<you>\psi-assessment\hadoop\bin;' + os.environ.get('PATH','')
import sys; sys.path.insert(0, 'src')
import pipeline; pipeline.main()
"
```

Expected output:

```
Pipeline completed successfully.
  Orphaned order_items : 489
  Return rate rows     : 36
[DQ WARN] 187 row(s) with net_amount < 0 (flagged via is_negative_amount)
```

### Running tests

```bash
pytest src/tests.py -v
```

---

## Pipeline Tasks

### Task 01 — Ingestion (`load_data`)

- Reads all four CSVs with explicit `StructType` schemas defined in `schemas.py`.
- Uses `mode=PERMISSIVE` so malformed rows are not silently dropped.
- Rows where the primary key column parsed to `NULL` are separated into a `rejected` DataFrame and logged.

### Task 02 — Cleaning (`clean_all`)

| Step | Detail |
|---|---|
| Deduplication | `dropDuplicates()` on all four tables |
| Date normalisation | `coalesce(to_date(col, 'yyyy-MM-dd'), to_date(col, 'dd/MM/yyyy'))` handles mixed formats; result stored as ISO string |
| NULL removal | Rows with `NULL` `order_id` or `customer_id` dropped from orders |
| Negative amount flag | `is_negative_amount` boolean column added; rows are **not** dropped per spec |
| Customer tier casing | `F.lower(customer_tier)` |

### Task 03 — Joins & Enrichment (`enrich`)

- `orders` → `customers` inner join (broadcast hint on customers — Bonus B2).
- `order_items` left-anti join against `orders` to isolate **orphaned items** (489 found).
- `orders + customers` → `order_items` inner join produces the enriched DataFrame.
- `net_amount = total_amount * (1 - discount_pct / 100)` added as a derived column.

### Task 04 — Window Analytics

| Function | Window | Output |
|---|---|---|
| `customer_lifetime_rank` | `DENSE_RANK` over `country`, ordered by `sum(net_amount) DESC` | `spend_rank` per customer per country |
| `rolling_order_counts` | `rangeBetween(-604800, 0)` seconds (7 days) per customer, ordered by Unix timestamp | `rolling_7d_order_count` |
| `category_revenue_share` | `sum(net_amount)` partitioned by `year_month` | `revenue_share_pct` per category per month |

All window functions use `pyspark.sql.Window` — no Python UDFs, no `.toPandas()`.

### Task 05 — Return Analysis (`return_analysis`)

- Returns joined to enriched orders on `order_id` (inner join).
- `refund_exceeds_order` flag: `refund_amount > net_amount`.
- Aggregated `return_rates` DataFrame: return count and `pct_exceeds_order` grouped by `(category, customer_tier)`.
- Result: 36 distinct `(category, customer_tier)` combinations.

### Task 06 — Output (`write_outputs`)

- **Enriched Parquet**: partitioned by `year` and `month`, written with `mode('overwrite')` to `output/enriched_orders/`. Spans 2022–2024 (30 partitions).
- **Top-10 refund customers**: aggregated by `sum(refund_amount)`, coalesced to a single CSV file at `output/top10_refund_customers/`.

---

## Data Quality

### Bonus B3 — DQ Gate

After enrichment, two checks run before any write:

| Check | Behaviour |
|---|---|
| `customer_id IS NULL` in enriched data | Raises `ValueError` — hard stop |
| `net_amount < 0` | Logs `[DQ WARN]` — pipeline continues (spec T02 says flag, not drop) |

The 187 rows with negative `net_amount` originate from negative `total_amount` values, which the spec explicitly requires to be flagged and retained.

### Known data issues handled

| Issue | Handling |
|---|---|
| ~8% exact duplicates | `dropDuplicates()` |
| Mixed date formats (`YYYY-MM-DD` / `DD/MM/YYYY`) | `coalesce(to_date(...), to_date(...))` |
| NULL `customer_id` / `total_amount` | NULLs on key columns dropped; `total_amount` NULLs propagate to `net_amount` |
| Negative `total_amount` | `is_negative_amount` flag; rows retained |
| Orphaned `order_items` | Isolated via `left_anti` join; 489 found |
| Inconsistent `customer_tier` casing | `F.lower()` |
| Extra `email` column in `customers.csv` | Added to schema; not used downstream |

---

## Outputs

```
output/
├── enriched_orders/
│   ├── year=2022/
│   │   ├── month=1/part-*.snappy.parquet
│   │   └── ...
│   ├── year=2023/
│   └── year=2024/
└── top10_refund_customers/
    └── part-00000-*.csv
```

Top refund customers (preview):

| customer_id | total_refund_amount |
|---|---|
| C00407 | 13,509.57 |
| C00306 | 11,760.85 |
| ... | ... |
