# Analysis: Debugging the Broken Stock Data Pipeline

**Lubo Bali**
**February 20, 2026**

---

## Diagnosis

### 1. How many tickers data do you expect? How many are actually there? Why?

I expect 8 tickers in the table (AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM). But when I run the broken script only 1 ticker shows up — JPM which is the last one in the list.

The reason is because the script uses `mode("overwrite")` inside the for loop. So every time it writes a new ticker it destroys whatever was there before. It goes through all 8 tickers but each one kills the previous. After the loop is done only JPM is left because it was the last one to write.

### 2. How many partition directories were created? Why is this a problem?

The script partitions by `("ticker", "minute")` where minute is formatted like `2026-02-18_09-30`. For 5 days of stock data thats around 1,950 minutes per ticker (6.5 market hours per day x 60 minutes x 5 days). With 8 tickers that could be up to 15,600 partition directories. Though because of bug 1 only JPM survives so we see around 1,950.

This is the small file problem. Every partition directory has one tiny parquet file maybe a few KB. Delta Lake is designed for files around 1 GB not 1 KB. When you have thousands of tiny files:
- Reading is slow because Spark has to open and close each file
- The metadata in `_delta_log` gets bloated tracking all these files
- The parquet headers and footers are bigger than the actual data inside

### 3. What is missing from the schema that makes time-based queries hard?

There is no `trade_date` column. The schema only has `timestamp_ms` which is the raw epoch time in milliseconds (a big number like 1708267800000). If you want to query "give me all data for February 18" you have to do math on this number every time like:

```sql
WHERE to_date(from_unixtime(timestamp_ms / 1000)) = '2026-02-18'
```

Thats ugly and also Spark cant use partition pruning on it. With a proper `trade_date` column you just write `WHERE trade_date = '2026-02-18'` and Spark skips all the partition folders that dont match. Much faster and much cleaner.

---

## What I Fixed

**Fix 1 — Collect all tickers then write once.** Instead of writing inside the loop with overwrite I collect bars from all 8 tickers into one big list first. Then I create one DataFrame and write it one time. All 8 tickers are preserved.

**Fix 2 — Partition by (ticker, trade_date) not (ticker, minute).** Daily partitions make way more sense for stock data. 8 tickers x 5 trading days = about 40 partitions. Thats manageable not thousands of tiny directories.

**Fix 3 — Added trade_date column.** I derive it from timestamp_ms like this:
```python
df.withColumn("trade_date", to_date((col("timestamp_ms") / 1000).cast("timestamp")))
```

Now you have a clean date column that works with partition pruning and is easy to query.

**Bonus — Rate limiting.** I added a 13 second delay between API calls because the Polygon API has a 5 calls per minute limit. The broken script had no delay at all so it would fail under concurrent usage. I also added retry logic for 429 (rate limit) responses.

---

## Partitioning Strategy

I went with traditional partitioning by `(ticker, trade_date)` because:

- The number of partitions is predictable — 8 tickers x however many trading days
- Stock queries almost always filter by ticker or date so this matches the query pattern
- It works both locally and on Databricks. Liquid clustering is Databricks only feature
- Its easy to see on disk whats happening when you look at the partition folders

Liquid clustering could also work here and its a cool feature but since we also run locally and partition by date is the standard way for time series data I think traditional partitioning is the right choice.

---

## Managed vs External Tables

| | Managed Table | External Table |
|---|---|---|
| Who owns the data | Databricks owns metadata AND data files | Databricks owns only metadata. You own the data files |
| What happens on DROP | Deletes everything — metadata and data gone | Only removes the catalog entry. Data files stay where they are |
| Where data lives | Databricks managed location | You pick the location (S3, ADLS, etc.) |
| Best for | Internal tables for your team | Shared data that multiple teams need |

For the stock data I would use an external table in production. Stock data is shared across many teams (trading, risk, analytics). If someone accidentally drops the table only the catalog entry goes away but the actual parquet files are still safe at the external location. Thats the whole point of external tables — you separate the catalog metadata from the actual data ownership.

I ran this locally so I used the simulated approach with a temporary view. On Databricks I would create a real external table in Unity Catalog so other teams can query it by name without knowing where the files actually live.
