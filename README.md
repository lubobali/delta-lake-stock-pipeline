# Homework: Debugging & Fixing a Poorly Partitioned Stock Data Pipeline

**Date:** February 20, 2026
**Covers:** Day 1 (Delta Lake Fundamentals) & Day 2 (Unity Catalog, Partitioning, External Tables)

---

## TL;DR

You are given a broken PySpark script (`broken_stock_harvester.py`) that fetches stock market data from the Massive.com API and writes it to a Delta table. The script has **three critical bugs** related to data ingestion and partitioning. Your job is to:

1. **Run the broken script** and observe the problems, make sure to consider rate limits of Massive.com
2. **Identify and fix all three bugs** in a new script called `fixed_stock_harvester.py`
3. **Run maintenance** on the corrected table (OPTIMIZE, VACUUM, health check) (or leverage liquid clustering)
4. **Create an external table** that makes the cleaned stock data queryable via Unity Catalog (or simulate locally)
5. **Write a short analysis** documenting what you found and why your fixes are correct

---

## Background

Your team ingests minute-level stock data from the [Massive.com Aggregates API](https://massive.com/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to) for a set of tickers (AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM). A junior engineer wrote the initial harvester script, but the resulting Delta table is broken and unusable. Leadership has asked you to fix it.

### API Details

- **Endpoint:** `GET /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}`
- **API Key:** `Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q` (already embedded in starter code 
- **Data returned:** OHLCV bars (open, high, low, close, volume) plus VWAP and transaction count

---

## Setup

### Local Environment

```bash
cd delta-table-homework
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Verify Java is available (`java -version`). PySpark requires Java 8 or 11.

### Databricks Environment

Upload the files to your workspace. The `databricks_utils.py` module auto-detects the environment and adjusts paths accordingly.

---

## Part 1: Run the Broken Script & Diagnose (20 points)

Run the starter code as-is:

```bash
python broken_stock_harvester.py
```

After it finishes, answer these questions in your `analysis.md` file:

1. **How many tickers' data do you expect in the table? How many are actually present? Why?**
2. **How many partition directories were created? Why is this a problem?**
3. **What is missing from the schema that would make time-based queries practical?**

---

## Part 2: Fix the Harvester (40 points)

Create a new file called `fixed_stock_harvester.py` that corrects all three bugs:

## Part 3: Table Maintenance (20 points)

After your fixed harvester writes the corrected table, add a maintenance section to your script (or a separate `maintenance.py`) that does the following:

### 3a. Run OPTIMIZE (5 points)

Compact the table's files. If you chose traditional partitioning, run basic compaction. If you chose no partitioning, consider Z-ORDER on `ticker` and `trade_date`.

```python
delta_table = DeltaTable.forPath(spark, table_path)
delta_table.optimize().executeCompaction()
# OR with Z-ORDER:
# delta_table.optimize().executeZOrderBy("ticker", "trade_date")
```

### 3b. Run VACUUM (5 points)

Clean up stale files. Use a 0-hour retention for this homework (production would use 168+ hours).

```python
spark.sql(f"VACUUM delta.`{table_path}` RETAIN 0 HOURS")
```

### 3c. Health Check (10 points)

Print a before/after comparison showing:
- Number of data files before and after OPTIMIZE
- Total table size
- Row count (verify no data was lost)
- Table history showing all operations

You may adapt the `DeltaTableHealthCheck` class from the Day 1 `07_maintenance_best_practices.py` lecture, or write your own.

---

## Part 4: Create an External Table (20 points)

Make the corrected stock data available as an **external table** so that other teams can query it without knowing the underlying file path.

### Databricks Environment (if available)

Register the Delta table as an external table in Unity Catalog:

```python
CATALOG = "main"
SCHEMA = "dataexpert"
EXTERNAL_LOCATION = "<your-external-location>"  # e.g., use s3://zachwilsonsorganization-522/external

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.stocks_external
    USING DELTA
    LOCATION '{EXTERNAL_LOCATION}/stocks_fixed'
    COMMENT 'Fixed stock OHLCV data from Polygon.io — external table'
""")
```

Then demonstrate querying through the catalog name:

```python
spark.sql(f"""
    SELECT ticker, trade_date, COUNT(*) as bar_count,
           MIN(low) as day_low, MAX(high) as day_high
    FROM {CATALOG}.{SCHEMA}.stocks_external
    GROUP BY ticker, trade_date
    ORDER BY ticker, trade_date
""").show(truncate=False)
```

### Local Environment (simulated)

If you don't have Databricks, simulate the external table pattern by:

1. Writing the fixed Delta table to a specific path (e.g., `./delta_tables/stocks_fixed`)
2. Creating a second "reference" that reads from that path using `spark.read.format("delta").load(path)`
3. Registering it as a temp view or SQL table:

```python
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW stocks_external
    AS SELECT * FROM delta.`{fixed_table_path}`
""")

spark.sql("""
    SELECT ticker, trade_date, COUNT(*) as bar_count,
           MIN(low) as day_low, MAX(high) as day_high
    FROM stocks_external
    GROUP BY ticker, trade_date
    ORDER BY ticker, trade_date
""").show(truncate=False)
```

Either approach is acceptable. Document which you used and explain the difference between managed and external tables (reference Day 2, Lecture 03).

---

## Deliverables

| File | Description |
|------|-------------|
| `fixed_stock_harvester.py` | Your corrected harvester script with all three bugs fixed |
| `maintenance.py` (or section in above) | OPTIMIZE, VACUUM, and health check code |
| `external_table.py` (or section in above) | External table creation and sample queries |
| `analysis.md` | Written answers to Part 1 questions + justification of your partitioning choice + managed vs. external table explanation |

---

### Massive.com API Key

The embedded key (`Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q`) is on the paid tier and performant:
- If many students use it at the same time, we need to gracefully handle the rate limits
- 5 API calls/minute
- Delayed data (15-minute delay for free tier)
- 5 years of historical data available
