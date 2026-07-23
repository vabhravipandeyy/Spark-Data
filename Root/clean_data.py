"""
clean_data.py

Cleans up the raw orders export before it goes anywhere near analytics.
The source file is a mess (it's exported straight from the ordering
system) — mixed casing, blank cells instead of real NULLs, cancelled
orders mixed in with real ones. This script fixes all that and drops
a tidy Parquet file ready for reporting.

Usage:
    python clean_data.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

RAW_PATH = "data/raw/orders.csv"
OUTPUT_PATH = "data/output/orders_clean.parquet"

# Defining the schema up front so Spark doesn't waste a pass guessing types.
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_date", StringType()),
    StructField("region", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("status", StringType()),
])


def load_raw(spark):
    return spark.read.option("header", "true").schema(schema).csv(RAW_PATH)


def clean(df):
    """Fix the messy bits and get everything into a usable shape."""

    df = (
        df.withColumnRenamed("customer_id", "cust_id")
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("region", F.initcap(F.trim("region")))
        .withColumn("status", F.upper(F.trim("status")))
    )

    # quantity/price/date are the fields we actually do math on, so if any
    # of those are missing the row isn't usable — drop it. Region missing
    # is fine, we just don't know where the order came from.
    df = df.na.drop(subset=["quantity", "unit_price", "order_date"])
    df = df.na.fill({"region": "Unknown"})

    # cancelled orders shouldn't count toward revenue numbers
    df = df.filter(F.col("status") != "CANCELLED")

    df = df.withColumn("order_value", F.round(F.col("quantity") * F.col("unit_price"), 2))

    return df


def main():
    spark = SparkSession.builder.appName("clean-orders").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = load_raw(spark)
    print(f"Read {raw.count()} raw rows")

    cleaned = clean(raw)
    print(f"{cleaned.count()} rows left after cleaning")

    cleaned.show(5)

    cleaned.write.mode("overwrite").partitionBy("region").parquet(OUTPUT_PATH)
    print(f"Saved to {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
