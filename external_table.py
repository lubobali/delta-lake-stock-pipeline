"""
External Table Creation
=======================

Make the corrected stock data queryable as an external table.

Supports both environments:
- Databricks: Creates an external table in Unity Catalog by reading
  the managed table's data location and registering a new table with
  CREATE TABLE ... USING DELTA LOCATION. This proves the "data lives
  outside the metastore" property — DROP TABLE removes only the catalog
  entry, the parquet files at LOCATION remain untouched.
- Local: Creates a true external table with USING DELTA LOCATION
  pointing to the absolute filesystem path of the Delta table.

Managed vs External tables:
- Managed: Databricks owns both metadata AND data files.
  DROP TABLE deletes everything (metadata + data).
- External: Databricks owns only metadata. Data files live at
  an external LOCATION you control (S3, ADLS, GCS, local path).
  DROP TABLE removes the catalog entry but data files remain untouched.
  Ideal for sharing data across teams without giving up ownership.
"""

import os
from delta import DeltaTable

try:
    from databricks_utils import get_spark, get_base_path, stop_spark_if_local, is_databricks
except ModuleNotFoundError:
    def is_databricks(): return True
    def get_spark(): return spark  # noqa: F821
    def get_base_path(subdir=""): return None
    def stop_spark_if_local(sp): pass

UC_TABLE = "tabular.dataexpert.lubobali_stocks_fixed"
UC_EXTERNAL = "tabular.dataexpert.lubobali_stocks_external"


def create_external_table_databricks(spark):
    """Create a true external table in Unity Catalog (Databricks environment).

    Reads the managed table's storage location via DESCRIBE DETAIL,
    then creates an external table pointing to that LOCATION.
    """
    print(f"Reading location of managed table {UC_TABLE}...")

    detail = spark.sql(f"DESCRIBE DETAIL {UC_TABLE}").collect()[0]
    data_location = detail['location']
    print(f"Managed table data location: {data_location}")

    # Drop any previous external table or view with this name
    spark.sql(f"DROP TABLE IF EXISTS {UC_EXTERNAL}")
    spark.sql(f"DROP VIEW IF EXISTS {UC_EXTERNAL}")

    print(f"\nCreating external table {UC_EXTERNAL}...")
    print(f"LOCATION: {data_location}")
    spark.sql(f"""
        CREATE TABLE {UC_EXTERNAL}
        USING DELTA
        LOCATION '{data_location}'
    """)

    print(f"External table created: {UC_EXTERNAL}")
    print("This is a TRUE external table — DROP TABLE removes only the")
    print("catalog entry. The parquet files at LOCATION remain untouched.")

    # Query through catalog name
    print("\nQuerying via Unity Catalog:")
    spark.sql(f"""
        SELECT ticker, trade_date, COUNT(*) as bar_count,
               MIN(low) as day_low, MAX(high) as day_high
        FROM {UC_EXTERNAL}
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """).show(truncate=False)

    # Additional analytics
    print("\nTop 5 highest volume days across all tickers:")
    spark.sql(f"""
        SELECT ticker, trade_date,
               SUM(volume) as total_volume,
               ROUND(AVG(vwap), 2) as avg_vwap
        FROM {UC_EXTERNAL}
        GROUP BY ticker, trade_date
        ORDER BY total_volume DESC
        LIMIT 5
    """).show(truncate=False)


def create_external_table_local(spark):
    """Create a true external table locally with USING DELTA LOCATION."""
    fixed_table_path = os.path.abspath(get_base_path("stocks_fixed"))
    print(f"Creating external table from location: {fixed_table_path}")

    # CREATE TABLE with LOCATION = true external table pattern.
    # The data files live at this path. DROP TABLE removes only
    # the catalog entry — files on disk remain untouched.
    spark.sql("DROP TABLE IF EXISTS stocks_external")
    spark.sql(f"""
        CREATE TABLE stocks_external
        USING DELTA
        LOCATION '{fixed_table_path}'
    """)

    print("External table 'stocks_external' created")
    print(f"Data LOCATION: {fixed_table_path}")
    print("DROP TABLE removes only the catalog entry.")
    print("The parquet files at LOCATION remain untouched.")

    print("\nQuerying via external table name:")
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
               ROUND(AVG(vwap), 2) as avg_vwap
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
