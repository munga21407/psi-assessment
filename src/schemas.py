from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)

orders_schema = StructType([
    StructField("order_id",     StringType(), True),
    StructField("customer_id",  StringType(), True),
    StructField("order_date",   StringType(), True),   # raw string; normalised in cleaning
    StructField("status",       StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
])

order_items_schema = StructType([
    StructField("item_id",    StringType(),  True),
    StructField("order_id",   StringType(),  True),
    StructField("product_id", StringType(),  True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
    StructField("category",   StringType(),  True),
])

customers_schema = StructType([
    StructField("customer_id",    StringType(), True),
    StructField("signup_date",    StringType(), True),  # raw string; normalised in cleaning
    StructField("country",        StringType(), True),
    StructField("customer_tier",  StringType(), True),
    StructField("email",          StringType(), True),
])

returns_schema = StructType([
    StructField("return_id",     StringType(), True),
    StructField("order_id",      StringType(), True),
    StructField("return_date",   StringType(), True),   # raw string; normalised in cleaning
    StructField("reason",        StringType(), True),
    StructField("refund_amount", DoubleType(), True),
])
