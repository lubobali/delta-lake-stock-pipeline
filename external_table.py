"""
External Table Creation
=======================

Make the corrected stock data queryable as an external table.

Supports both environments:
- Databricks: Creates an external table in Unity Catalog
- Local: Simulates with a temporary view registered via SQL

Managed vs External tables:
- Managed: Databricks owns both metadata AND data files.
  DROP TABLE deletes everything (metadata + data).
- External: Databricks owns only metadata. Data files live at
  an external LOCATION you control (S3, ADLS, GCS, local path).
  DROP TABLE removes the catalog entry but data files remain untouched.
  Ideal for sharing data across teams without giving up ownership.
"""

from delta import DeltaTable

try:
    from databricks_utils import get_spark, get_base_path, stop_spark_if_local, is_databricks
except ModuleNotFoundError:
    def is_databricks(): return True
    def get_spark(): return spark  # noqa: F821
    def get_base_path(subdir=""):
        base = "/Volumes/tabular/default/delta_stock_pipeline"
        return f"{base}/{subdir}" if subdir else base
    def stop_spark_if_local(sp): pass


def create_external_table_databricks(spark, fixed_table_path):
    """Create an external table in Unity Catalog (Databricks environment)."""
    CATALOG = "tabular"
    SCHEMA = "stocks"

    print(f"Creating external table in {CATALOG}.{SCHEMA}...")

    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.stocks_external
        USING DELTA
        LOCATION '{fixed_table_path}'
        COMMENT 'Fixed stock OHLCV data from Polygon.io — external table'
    """)

    print(f"External table created: {CATALOG}.{SCHEMA}.stocks_external")

    # Query through catalog name
    print("\nQuerying via Unity Catalog:")
    spark.sql(f"""
        SELECT ticker, trade_date, COUNT(*) as bar_count,
               MIN(low) as day_low, MAX(high) as day_high
        FROM {CATALOG}.{SCHEMA}.stocks_external
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """).show(truncate=False)


def create_external_table_local(spark, fixed_table_path):
    """Simulate external table pattern locally with a temp view."""
    print(f"Creating simulated external table from: {fixed_table_path}")

    # Register a temp view pointing to the Delta table path
    # This simulates what an external table does: other consumers query
    # by name without knowing the underlying file path.
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW stocks_external
        AS SELECT * FROM delta.`{fixed_table_path}`
    """)

    print("Temporary view 'stocks_external' created")

    # Query through the view name (just like querying a catalog table)
    print("\nQuerying via view name (simulates Unity Catalog access):")
    spark.sql("""
        SELECT ticker, trade_date, COUNT(*) as bar_count,
               MIN(low) as day_low, MAX(high) as day_high
        FROM stocks_external
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """).show(truncate=False)

    # Show additional analytics to demonstrate the table works
    print("\nTop 5 highest volume days across all tickers:")
    spark.sql("""
        SELECT ticker, trade_date,
               SUM(volume) as total_volume,
               AVG(vwap) as avg_vwap
        FROM stocks_external
        GROUP BY ticker, trade_date
        ORDER BY total_volume DESC
        LIMIT 5
    """).show(truncate=False)

    print("\nSchema of the external table:")
    spark.sql("DESCRIBE stocks_external").show(truncate=False)


def main():
    spark = get_spark()

    fixed_table_path = get_base_path("stocks_fixed")

    print("=" * 60)
    print("EXTERNAL TABLE CREATION")
    print("=" * 60)

    # Verify the fixed table exists
    try:
        DeltaTable.forPath(spark, fixed_table_path)
    except Exception:
        print(f"\nError: Fixed table not found at {fixed_table_path}")
        print("Run fixed_stock_harvester.py first!")
        stop_spark_if_local(spark)
        return

    if is_databricks():
        create_external_table_databricks(spark, fixed_table_path)
    else:
        create_external_table_local(spark, fixed_table_path)

    stop_spark_if_local(spark)
    print("\nExternal table creation completed!")


if __name__ == "__main__":
    main()
