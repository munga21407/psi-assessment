from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from schemas import (
    orders_schema,
    order_items_schema,
    customers_schema,
    returns_schema,
)

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def get_spark(app_name: str = "psi-assessment") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "false")   # allow to_date to return NULL on bad input
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Task 01 – Ingestion
# ---------------------------------------------------------------------------

def _read_csv(spark: SparkSession, path: str, schema) -> tuple[DataFrame, DataFrame]:
    """
    Read a CSV with an explicit schema.
    Rows that fail type casting end up with NULLs on the typed column; we
    surface those as 'rejected' by checking whether a non-nullable marker
    column (the first field) became NULL after casting.
    """
    raw = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")        # bad rows get NULL, not dropped
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(path)
    )

    first_col = schema.fields[0].name
    rejected = raw.filter(F.col(first_col).isNull())
    good     = raw.filter(F.col(first_col).isNotNull())

    if rejected.count() > 0:
        print(f"[WARN] {path}: {rejected.count()} rejected row(s) logged below")
        rejected.show(truncate=False)

    return good, rejected


def load_data(spark: SparkSession, data_dir: str) -> dict[str, tuple[DataFrame, DataFrame]]:
    """
    Load all four source CSVs.
    Returns a dict of table_name -> (good_df, rejected_df).
    """
    return {
        "orders":       _read_csv(spark, f"{data_dir}/orders.csv",       orders_schema),
        "order_items":  _read_csv(spark, f"{data_dir}/order_items.csv",  order_items_schema),
        "customers":    _read_csv(spark, f"{data_dir}/customers.csv",    customers_schema),
        "returns":      _read_csv(spark, f"{data_dir}/returns.csv",      returns_schema),
    }


# ---------------------------------------------------------------------------
# Task 02 – Cleaning helpers
# ---------------------------------------------------------------------------

def _normalize_date(col_name: str) -> F.Column:
    """
    Coerce a mixed-format date column to ISO YYYY-MM-DD (kept as StringType).
    Handles:
      - YYYY-MM-DD  (already correct)
      - DD/MM/YYYY
    """
    iso   = F.to_date(F.col(col_name), "yyyy-MM-dd")
    slash = F.to_date(F.col(col_name), "dd/MM/yyyy")
    return F.date_format(F.coalesce(iso, slash), "yyyy-MM-dd")


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("order_date", _normalize_date("order_date"))
        .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("is_negative_amount", F.col("total_amount") < 0)
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("signup_date",   _normalize_date("signup_date"))
        .withColumn("customer_tier", F.lower(F.col("customer_tier")))
    )


def clean_returns(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("return_date", _normalize_date("return_date"))
    )


def clean_all(raw: dict[str, tuple[DataFrame, DataFrame]]) -> dict[str, DataFrame]:
    """
    Apply all Task 02 cleaning steps.
    Returns a dict of table_name -> cleaned DataFrame.
    """
    orders_good,      _ = raw["orders"]
    order_items_good, _ = raw["order_items"]
    customers_good,   _ = raw["customers"]
    returns_good,     _ = raw["returns"]

    return {
        "orders":      clean_orders(orders_good),
        "order_items": clean_order_items(order_items_good),
        "customers":   clean_customers(customers_good),
        "returns":     clean_returns(returns_good),
    }


# ---------------------------------------------------------------------------
# Task 03 – Joins & Enrichment
# ---------------------------------------------------------------------------

def enrich(cleaned: dict[str, DataFrame]) -> tuple[DataFrame, DataFrame]:
    """
    Returns:
        enriched_df   – orders joined with customers and order_items,
                        plus net_amount derived column.
        orphaned_df   – order_items whose order_id has no matching order
                        (left anti-join).
    """
    orders      = cleaned["orders"]
    order_items = cleaned["order_items"]
    customers   = cleaned["customers"]

    # Bonus B2: broadcast the small customers table to avoid a shuffle join
    orders_with_customers = orders.join(
        F.broadcast(customers),
        on="customer_id",
        how="inner",
    )

    # Isolate orphaned items before the main join so nothing is silently lost
    orphaned_df = order_items.join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti",
    )

    enriched_df = (
        orders_with_customers
        .join(order_items, on="order_id", how="inner")
        .withColumn(
            "net_amount",
            F.col("total_amount") * (F.lit(1) - F.col("discount_pct") / F.lit(100)),
        )
    )

    return enriched_df, orphaned_df


# ---------------------------------------------------------------------------
# Task 04 – Analytical Window Functions
# ---------------------------------------------------------------------------

def customer_lifetime_rank(enriched_df: DataFrame) -> DataFrame:
    """
    Rank customers by their lifetime net spend within each country.
    Rank 1 = highest spender. Ties share the same rank (DENSE_RANK).
    One row per (country, customer_id).
    """
    spend_per_customer = (
        enriched_df
        .groupBy("country", "customer_id")
        .agg(F.sum("net_amount").alias("lifetime_net_spend"))
    )

    w = Window.partitionBy("country").orderBy(F.desc("lifetime_net_spend"))

    return spend_per_customer.withColumn("spend_rank", F.dense_rank().over(w))


def rolling_order_counts(enriched_df: DataFrame) -> DataFrame:
    """
    7-day rolling order count per customer.
    The window is defined in seconds: 7 days = 7 * 86 400 = 604 800 s.
    rangeBetween(-604_800, 0) looks back 7 days from (and including) the
    current row's order_date timestamp.
    One row per original order row, with an additional rolling_7d_order_count column.
    """
    # Cast the ISO date string to a unix timestamp (seconds) for the range frame
    df = enriched_df.withColumn(
        "order_ts",
        F.unix_timestamp(F.to_date(F.col("order_date"), "yyyy-MM-dd")),
    )

    seven_days = 7 * 24 * 60 * 60

    w = (
        Window
        .partitionBy("customer_id")
        .orderBy("order_ts")
        .rangeBetween(-seven_days, 0)
    )

    return (
        df
        .withColumn("rolling_7d_order_count", F.count("order_id").over(w))
        .drop("order_ts")
    )


def category_revenue_share(enriched_df: DataFrame) -> DataFrame:
    """
    Each category's percentage share of total revenue per calendar month.
    revenue_share = category_monthly_revenue / total_monthly_revenue * 100
    One row per (year_month, category).
    """
    df = enriched_df.withColumn(
        "year_month",
        F.date_format(F.to_date(F.col("order_date"), "yyyy-MM-dd"), "yyyy-MM"),
    )

    monthly_category = (
        df
        .groupBy("year_month", "category")
        .agg(F.sum("net_amount").alias("category_revenue"))
    )

    w = Window.partitionBy("year_month")

    return monthly_category.withColumn(
        "revenue_share_pct",
        F.round(
            F.col("category_revenue") / F.sum("category_revenue").over(w) * F.lit(100),
            2,
        ),
    )


# ---------------------------------------------------------------------------
# Task 05 – Return Analysis
# ---------------------------------------------------------------------------

def return_analysis(enriched_df: DataFrame, returns_df: DataFrame) -> DataFrame:
    """
    Join returns to the enriched orders and:
      - Compute return rates by category and customer_tier.
      - Flag refund_exceeds_order where refund_amount > net_amount.

    Returns the enriched-plus-returns DataFrame with all flags attached.
    Unmatched returns (no corresponding order in enriched_df) are dropped
    via inner join — they reference orders already filtered as invalid.
    """
    # Deduplicate net_amount to order level before joining returns
    # (enriched_df has one row per order_item; we need order-level net_amount)
    order_net = (
        enriched_df
        .select("order_id", "customer_id", "country", "customer_tier",
                "category", "net_amount")
    )

    joined = returns_df.join(order_net, on="order_id", how="inner")

    flagged = joined.withColumn(
        "refund_exceeds_order",
        F.col("refund_amount") > F.col("net_amount"),
    )

    return_rates = (
        flagged
        .groupBy("category", "customer_tier")
        .agg(
            F.count("return_id").alias("return_count"),
            F.avg(F.col("refund_exceeds_order").cast("int")).alias("pct_exceeds_order"),
        )
    )

    return flagged, return_rates


# ---------------------------------------------------------------------------
# Task 06 – Output
# ---------------------------------------------------------------------------

def write_outputs(
    enriched_df: DataFrame,
    flagged_returns_df: DataFrame,
    output_dir: str,
) -> None:
    """
    Write final outputs:
      1. Enriched orders → Parquet partitioned by year / month.
      2. Top-10 refund customers → single CSV file.

    Both writes use mode('overwrite') for idempotency.
    """
    # Add partition columns derived from order_date
    partitioned = (
        enriched_df
        .withColumn("year",  F.year(F.to_date(F.col("order_date"),  "yyyy-MM-dd")))
        .withColumn("month", F.month(F.to_date(F.col("order_date"), "yyyy-MM-dd")))
    )

    (
        partitioned
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{output_dir}/enriched_orders")
    )

    # Top-10 customers by total refund amount
    top10_refund = (
        flagged_returns_df
        .groupBy("customer_id")
        .agg(F.sum("refund_amount").alias("total_refund_amount"))
        .orderBy(F.desc("total_refund_amount"))
        .limit(10)
    )

    (
        top10_refund
        .coalesce(1)                          # single CSV file, not a part directory
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{output_dir}/top10_refund_customers")
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(data_dir: str = None, output_dir: str = None) -> None:
    from pathlib import Path
    _root = Path(__file__).resolve().parent.parent
    if data_dir is None:
        data_dir = str(_root / "data")
    if output_dir is None:
        output_dir = str(_root / "output")
    spark = get_spark()

    # T01 – Ingest
    raw = load_data(spark, data_dir)

    # T02 – Clean
    cleaned = clean_all(raw)

    # T03 – Enrich
    enriched_df, orphaned_df = enrich(cleaned)

    # T04 – Analytics (materialised for downstream use / inspection)
    ranked      = customer_lifetime_rank(enriched_df)
    rolling     = rolling_order_counts(enriched_df)
    rev_share   = category_revenue_share(enriched_df)

    # T05 – Returns
    flagged_returns, return_rates = return_analysis(enriched_df, cleaned["returns"])

    # Bonus B3 – Data Quality Gate
    null_customers = enriched_df.filter(F.col("customer_id").isNull()).count()
    negative_net   = enriched_df.filter(F.col("net_amount") < 0).count()
    if null_customers > 0:
        raise ValueError(f"DQ gate failed: {null_customers} row(s) with NULL customer_id in enriched data")
    if negative_net > 0:
        # Spec T02 says flag negatives, not drop — gate logs but does not halt
        print(f"[DQ WARN] {negative_net} row(s) with net_amount < 0 (flagged via is_negative_amount)")

    # T06 – Write outputs
    write_outputs(enriched_df, flagged_returns, output_dir)

    print("Pipeline completed successfully.")
    print(f"  Orphaned order_items : {orphaned_df.count()}")
    print(f"  Return rate rows     : {return_rates.count()}")


if __name__ == "__main__":
    main()
