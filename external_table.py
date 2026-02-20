"""
External Table Creation
=======================

Make the corrected stock data queryable as an external table.

Supports both environments:
- Databricks: The fixed table is already a managed table in Unity Catalog.
  We create a view to demonstrate the external access pattern.
- Local: Simulates with a temporary view registered via SQL.

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
    def get_base_path(subdir=""): return None
    def stop_spark_if_local(sp): pass

UC_TABLE = "main.default.lubobali_stocks_fixed"
UC_VIEW = "main.default.lubobali_stocks_external"


def create_external_table_databricks(spark):
    """Create an external view in Unity Catalog (Databricks environment)."""
    print(f"Creating view {UC_VIEW} from managed table {UC_TABLE}...")

    spark.sql(f"DROP VIEW IF EXISTS {UC_VIEW}")
    spark.sql(f"""
        CREATE VIEW {UC_VIEW}
        AS SELECT * FROM {UC_TABLE}
    """)

    print(f"View created: {UC_VIEW}")

    # Query through catalog name
    print("\nQuerying via Unity Catalog:")
    spark.sql(f"""
        SELECT ticker, trade_date, COUNT(*) as bar_count,
               MIN(low) as day_low, MAX(high) as day_high
        FROM {UC_VIEW}
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """).show(truncate=False)

    # Additional analytics
    print("\nTop 5 highest volume days across all tickers:")
    spark.sql(f"""
        SELECT ticker, trade_date,
               SUM(volume) as total_volume,
               ROUND(AVG(vwap), 2) as avg_vwap
        FROM {UC_VIEW}
        GROUP BY ticker, trade_date
        ORDER BY total_volume DESC
        LIMIT 5
    """).show(truncate=False)


def create_external_table_local(spark):
    """Simulate external table pattern locally with a temp view."""
    fixed_table_path = get_base_path("stocks_fixed")
    print(f"Creating simulated external table from: {fixed_table_path}")

    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW stocks_external
        AS SELECT * FROM delta.`{fixed_table_path}`
    """)

    print("Temporary view 'stocks_external' created")

    print("\nQuerying via view name (simulates Unity Catalog access):")
    spark.sql("""
        SELECT ticker, trade_date, COUNT(*) as bar_count,
               MIN(low) as day_low, MAX(high) as day_high
        FROM stocks_external
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """).show(truncate=False)

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

    print("=" * 60)
    print("EXTERNAL TABLE CREATION")
    print("=" * 60)

    if is_databricks():
        create_external_table_databricks(spark)
    else:
        create_external_table_local(spark)

    stop_spark_if_local(spark)
    print("\nExternal table creation completed!")


if __name__ == "__main__":
    main()
