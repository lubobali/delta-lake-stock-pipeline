# Delta Lake Stock Pipeline

Stock market data pipeline built with PySpark and Delta Lake. Ingests minute-level OHLCV data from Polygon.io for 8 major tickers, writes to a properly partitioned Delta table, runs table maintenance, and registers as an external table via Unity Catalog.

## Architecture

```
Polygon.io API  →  PySpark  →  Delta Table (partitioned by ticker + trade_date)
                                    ↓
                              OPTIMIZE + Z-ORDER + VACUUM
                                    ↓
                              External Table (Unity Catalog)
```

## What This Pipeline Does

1. **Data Ingestion** — Fetches minute-level OHLCV bars (open, high, low, close, volume, VWAP) from Polygon.io REST API for AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM
2. **Delta Table Storage** — Writes to a Delta table partitioned by `(ticker, trade_date)` with proper schema including derived `trade_date` column
3. **Table Maintenance** — Runs OPTIMIZE with Z-ORDER on `timestamp_ms`, VACUUM for storage cleanup, and before/after health checks
4. **External Table** — Registers the Delta table in Unity Catalog so other teams can query by name without knowing file paths

## Tech Stack

- **PySpark** — Distributed data processing
- **Delta Lake** — ACID transactions, time travel, schema enforcement
- **Polygon.io API** — Real-time and historical stock market data
- **Unity Catalog** — Data governance and external table registration
- **Databricks** — Cloud runtime (also runs locally)

## Pipeline Scripts

| Script | Description |
|--------|-------------|
| `stock_harvester.py` | Main pipeline — ingests stock data, writes partitioned Delta table |
| `maintenance.py` | OPTIMIZE, Z-ORDER, VACUUM, before/after health check |
| `external_table.py` | External table creation (Unity Catalog + local simulation) |
| `databricks_utils.py` | Environment detection — auto-switches between local PySpark and Databricks |
| `DESIGN.md` | Technical writeup — partitioning strategy, managed vs external tables |

## Key Design Decisions

**Partitioning: `(ticker, trade_date)`** — 8 tickers x N trading days = predictable partition count. Stock queries almost always filter by ticker or date, so partition pruning kicks in naturally.

**Rate Limiting** — Polygon.io has a 5 calls/minute limit. Pipeline adds 13s delay between API calls + automatic retry on 429 responses.

**`trade_date` column** — Derived from raw `timestamp_ms` (epoch milliseconds) to enable clean time-based queries and partition pruning instead of casting on every query.

## Setup

### Local
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
java -version  # PySpark requires Java 8 or 11
```

### Databricks
Upload files to workspace or connect as Git Folder. `databricks_utils.py` auto-detects the environment.

## Run

```bash
# 1. Ingest stock data (~2 min due to rate limiting)
python stock_harvester.py

# 2. Run maintenance (OPTIMIZE + VACUUM + health check)
python maintenance.py

# 3. Create external table and run sample queries
python external_table.py
```

## Sample Output

```
Rows per ticker:
+------+-----+
|ticker|count|
+------+-----+
|  AAPL| 1950|
|  AMZN| 1950|
|  GOOGL| 1950|
|  JPM | 1950|
|  META | 1950|
|  MSFT | 1950|
|  NVDA | 1950|
|  TSLA | 1950|
+------+-----+
```

## Design Decisions

See [DESIGN.md](DESIGN.md) for the full technical writeup covering partitioning strategy, managed vs external tables, and performance considerations.

| Decision | Rationale |
|----------|-----------|
| `partitionBy("ticker", "trade_date")` | Matches query patterns — stock queries almost always filter by ticker or date. ~40 partitions vs thousands |
| Z-ORDER on `timestamp_ms` | Partition columns already get pruned automatically. Z-ORDER on timestamp enables file-level skipping for time-range queries |
| Derived `trade_date` column | Clean date column from raw epoch ms — enables partition pruning and readable queries |
| 13s API delay + 429 retry | Polygon.io 5 calls/min limit. Fail-safe with automatic backoff |
