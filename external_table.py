"""
External Table Creation
=======================

Make the corrected stock data queryable as an external table.

Managed vs External tables:
- Managed: Databricks owns both metadata AND data files.
  DROP TABLE deletes everything (metadata + data).
- External: Databricks owns only metadata. Data files live at
  an external LOCATION you control (S3, ADLS, GCS, local path).
  DROP TABLE removes the catalog entry but data files remain untouched.
  Ideal for sharing data across teams without giving up ownership.

Supports both environments:
- Databricks: Attempts to create a true external table via
  CREATE TABLE ... USING DELTA LOCATION. Requires an external location
  and storage credential in Unity Catalog. If unavailable, falls back
  to a view and explains why.
- Local: Creates a true external table with USING DELTA LOCATION
  pointing to the absolute filesystem path of the Delta table.
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
    """Create an external table in Unity Catalog (Databricks environment).

    A true external table requires an external location and storage
    credential configured in Unity Catalog. If not available, we fall
    back to a view and show the CREATE TABLE ... LOCATION SQL that
    would work with proper external storage.
    """
    # Clean up any previous object with this name
    spark.sql(f"DROP VIEW IF EXISTS {UC_EXTERNAL}")
    spark.sql(f"DROP TABLE IF EXISTS {UC_EXTERNAL}")

    # Get managed table location to show the architecture
    detail = spark.sql(f"DESCRIBE DETAIL {UC_TABLE}").collect()[0]
    managed_location = detail['location']
    print(f"Managed table location: {managed_location}")

    # Attempt to create a true external table
    # This requires an external location + storage credential in UC
    external_created = False
    try:
        # In production, LOCATION would point to your own S3/ADLS bucket:
        # CREATE TABLE ... USING DELTA LOCATION 's3://my-bucket/stocks_fixed'
        # Here we attempt using the managed table's location to demonstrate:
        spark.sql(f"""
            CREATE TABLE {UC_EXTERNAL}
            USING DELTA
            LOCATION '{managed_location}'
        """)
        external_created = True
        print(f"\nExternal table created: {UC_EXTERNAL}")
        print(f"LOCATION: {managed_location}")
    except Exception as e:
        error_msg = str(e)
        if "LOCATION_OVERLAP" in error_msg or "INVALID_PARAMETER_VALUE" in error_msg:
            print("\nCannot create external table pointing to managed storage.")
            print("This environment has no external location or storage credential.")
            print("In production with an external S3/ADLS bucket, the SQL would be:")
            print(f"""
    CREATE TABLE {UC_EXTERNAL}
    USING DELTA
    LOCATION 's3://my-external-bucket/stocks_fixed'
            """)
            print("Falling back to a view for query demonstration...\n")
            spark.sql(f"""
                CREATE VIEW {UC_EXTERNAL}
                AS SELECT * FROM {UC_TABLE}
            """)
            print(f"View created: {UC_EXTERNAL}")
        else:
            raise

    # Explain the difference
    if external_created:
        print("This is a TRUE external table:")
        print("  - DROP TABLE removes only the catalog entry")
        print("  - Parquet files at LOCATION remain untouched")
        print("  - Other teams query by name without knowing file paths")
    else:
        print("With a true external table (when external location is available):")
        print("  - DROP TABLE removes only the catalog entry")
        print("  - Parquet files at LOCATION remain untouched")
        print("  - Other teams query by name without knowing file paths")

    # Query through catalog name
    print("\nQuerying via Unity Catalog:")
    spark.sql(f"""
        SELECT ticker, trade_date, COUNT(*) as bar_count,
               MIN(low) as day_low, MAX(high) as day_high
        FROM {UC_EXTERNAL}
        GROUP BY ticker, trade_date
        ORDER BY ticker, trade_date
    """).show(truncate=False)

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
    """Create a true external table locally with USING DELTA LOCATION.

    This is the real external table pattern — CREATE TABLE with LOCATION
    points to data files on disk. DROP TABLE removes only the catalog
    entry; the parquet files at LOCATION remain untouched.
    """
    fixed_table_path = os.path.abspath(get_base_path("stocks_fixed"))
    print(f"Creating external table from location: {fixed_table_path}")

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
