"""
Stock Data Harvester - FIXED
============================

Fixed version of the broken stock harvester with all three bugs corrected:

Bug 1: mode("overwrite") inside the for loop — each ticker overwrote the previous.
       Fix: Collect ALL ticker data first, then write once.

Bug 2: Partitioning by ("ticker", "minute") — created thousands of tiny partition
       directories (one per minute per ticker). Classic small file problem.
       Fix: Partition by ("ticker", "trade_date") for daily granularity.

Bug 3: No trade_date column — only raw timestamp_ms (epoch milliseconds).
       Time-based queries were impractical without a human-readable date.
       Fix: Derive trade_date from timestamp_ms.

Data Source: Polygon.io REST API (Aggregates endpoint)
"""

import requests
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, expr, to_date, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType
)
from delta import DeltaTable

from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local, is_databricks

POLYGON_API_KEY = "Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q"
POLYGON_BASE_URL = "https://aggs.polygon.io/v2/aggs"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM"]

# Rate limit: 5 API calls per minute
API_CALL_DELAY_SECONDS = 13  # 60s / 5 calls = 12s minimum, use 13s for safety

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

    FIXES APPLIED:
    1. Collect ALL tickers first, then write ONCE (was: overwrite per ticker)
    2. Partition by (ticker, trade_date) not (ticker, minute)
    3. Derive trade_date column from timestamp_ms
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    from_str = start_date.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")

    print(f"Harvesting stock data from {from_str} to {to_str}")
    print(f"Tickers: {TICKERS}")

    # FIX #1: Collect ALL ticker data into one list before writing
    all_bars = []

    for i, ticker in enumerate(TICKERS):
        print(f"\nFetching {ticker}...")

        # Rate limiting: wait between API calls (5 calls/minute limit)
        if i > 0:
            print(f"  Waiting {API_CALL_DELAY_SECONDS}s for rate limit...")
            time.sleep(API_CALL_DELAY_SECONDS)

        try:
            bars = fetch_stock_data(ticker, from_str, to_str)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"  Rate limited! Waiting 60s and retrying {ticker}...")
                time.sleep(60)
                bars = fetch_stock_data(ticker, from_str, to_str)
            else:
                print(f"  Error fetching {ticker}: {e}")
                continue

        if not bars:
            continue

        print(f"  Got {len(bars)} bars for {ticker}")
        all_bars.extend(bars)

    if not all_bars:
        print("No data fetched for any ticker!")
        return

    print(f"\nTotal bars collected: {len(all_bars)}")

    # Create DataFrame from ALL collected data
    df = spark.createDataFrame(all_bars, schema=STOCK_SCHEMA)

    # FIX #3: Add trade_date column derived from timestamp_ms
    df_with_date = df.withColumn(
        "trade_date",
        to_date((col("timestamp_ms") / 1000).cast("timestamp"))
    )

    # FIX #2: Partition by (ticker, trade_date) instead of (ticker, minute)
    # Daily partitions = manageable number of directories
    # 8 tickers * ~5 trading days = ~40 partitions (not thousands)
    df_with_date.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("ticker", "trade_date") \
        .save(table_path)

    print(f"\nWrote {len(all_bars)} total bars to {table_path}")
    print(f"Tickers: {df_with_date.select('ticker').distinct().count()}")
    print(f"Date range: {df_with_date.agg({'trade_date': 'min'}).collect()[0][0]} to "
          f"{df_with_date.agg({'trade_date': 'max'}).collect()[0][0]}")


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

    print("\nRows per trade_date:")
    df.groupBy("trade_date").count().orderBy("trade_date").show()

    print("\nSample data:")
    df.select("ticker", "open", "close", "volume", "timestamp_ms", "trade_date") \
        .orderBy("ticker", "timestamp_ms") \
        .show(20, truncate=False)


def main():
    spark = get_spark()

    table_path = get_base_path("stocks_fixed")
    cleanup_path(spark, table_path)

    print("=" * 60)
    print("STOCK DATA HARVESTER (FIXED)")
    print("=" * 60)

    harvest_and_store(spark, table_path, days_back=5)

    analyze_table(spark, table_path)

    stop_spark_if_local(spark)
    print("\nFixed harvester completed!")


if __name__ == "__main__":
    main()
