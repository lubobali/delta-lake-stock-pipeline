"""
Stock Data Harvester - BROKEN (Homework Starter Code)
=====================================================

This script fetches stock market data from the Polygon.io API and stores
it in a Delta table. However, there are SEVERAL issues with how the data
is being written and partitioned.

YOUR TASK: Identify and fix the problems in this script.

Data Source: Polygon.io REST API (Aggregates endpoint)
"""

import requests
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, expr, to_date, date_format, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, TimestampType
)
from delta import DeltaTable

from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local, is_databricks

POLYGON_API_KEY = "Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q"
POLYGON_BASE_URL = "https://aggs.polygon.io/v2/aggs"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM"]

STOCK_SCHEMA = StructType([
    StructField("ticker", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("timestamp_ms", LongType(), True),
    StructField("num_transactions", IntegerType(), True),
])


def fetch_stock_data(ticker, from_date, to_date):
    """
    Fetch aggregate bar data from Polygon.io for a given ticker.

    Returns a list of dicts with OHLCV data.
    """
    url = (
        f"{POLYGON_BASE_URL}/ticker/{ticker}"
        f"/range/1/minute/{from_date}/{to_date}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    if data.get("resultsCount", 0) == 0:
        print(f"  No results for {ticker}")
        return []

    results = []
    for bar in data.get("results", []):
        results.append({
            "ticker": ticker,
            "open": bar.get("o"),
            "high": bar.get("h"),
            "low": bar.get("l"),
            "close": bar.get("c"),
            "volume": bar.get("v"),
            "vwap": bar.get("vw"),
            "timestamp_ms": bar.get("t"),
            "num_transactions": bar.get("n"),
        })

    return results


def harvest_and_store(spark, table_path, days_back=5):
    """
    Harvest stock data and write to a Delta table.
    FIX THE BUGS
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    from_str = start_date.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")

    print(f"Harvesting stock data from {from_str} to {to_str}")
    print(f"Tickers: {TICKERS}")

    for ticker in TICKERS:
        print(f"\nFetching {ticker}...")
        bars = fetch_stock_data(ticker, from_str, to_str)

        if not bars:
            continue

        print(f"  Got {len(bars)} bars for {ticker}")

        df = spark.createDataFrame(bars, schema=STOCK_SCHEMA)

        df_with_minute = df.withColumn(
            "minute",
            date_format(
                (col("timestamp_ms") / 1000).cast("timestamp"),
                "yyyy-MM-dd_HH-mm"
            )
        )

        df_with_minute.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("ticker", "minute") \
            .save(table_path)

        print(f"  Wrote {ticker} data to {table_path}")

def analyze_table(spark, table_path):
    """Read back the table and show what we have."""
    print("\n" + "=" * 60)
    print("TABLE ANALYSIS")
    print("=" * 60)

    df = spark.read.format("delta").load(table_path)

    print(f"\nTotal rows: {df.count()}")
    print(f"\nSchema:")
    df.printSchema()

    print("\nRows per ticker:")
    df.groupBy("ticker").count().orderBy("ticker").show()

    print("\nSample data:")
    df.select("ticker", "open", "close", "volume", "timestamp_ms", "minute") \
        .orderBy("ticker", "timestamp_ms") \
        .show(20, truncate=False)


def main():
    spark = get_spark()

    table_path = get_base_path("stocks_raw")
    cleanup_path(spark, table_path)

    print("=" * 60)
    print("STOCK DATA HARVESTER (BROKEN)")
    print("=" * 60)

    harvest_and_store(spark, table_path, days_back=5)

    analyze_table(spark, table_path)

    stop_spark_if_local(spark)
    print("\n✓ Harvester completed (with bugs!)")


if __name__ == "__main__":
    main()
