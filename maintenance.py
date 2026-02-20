"""
Stock Table Maintenance
=======================

Run maintenance on the corrected stock Delta table.
- OPTIMIZE with Z-ORDER on ticker + trade_date
- VACUUM (0 hours for demo, 168+ in production)
- Before/after health check comparison

Implements Delta Lake maintenance best practices
"""

import os
import time
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


class StockTableHealthCheck:
    """Health check utility for the stock Delta table."""

    def __init__(self, spark, table_path):
        self.spark = spark
        self.table_path = table_path
        self.delta_table = DeltaTable.forPath(spark, table_path)

    def get_file_statistics(self):
        """Get statistics about data files."""
        if is_databricks():
            detail = self.spark.sql(
                f"DESCRIBE DETAIL delta.`{self.table_path}`"
            ).collect()[0]
            num_files = detail['numFiles']
            total_size = detail['sizeInBytes']
            if num_files == 0:
                return {'num_files': 0, 'total_size_mb': 0, 'avg_size_mb': 0}
            return {
                'num_files': num_files,
                'total_size_mb': total_size / (1024 * 1024),
                'avg_size_mb': (total_size / num_files) / (1024 * 1024),
            }
        else:
            files = []
            for root, dirs, filenames in os.walk(self.table_path):
                if '_delta_log' in root:
                    continue
                for f in filenames:
                    if f.endswith('.parquet'):
                        path = os.path.join(root, f)
                        files.append(os.path.getsize(path))

            if not files:
                return {'num_files': 0, 'total_size_mb': 0, 'avg_size_mb': 0}

            return {
                'num_files': len(files),
                'total_size_mb': sum(files) / (1024 * 1024),
                'avg_size_mb': (sum(files) / len(files)) / (1024 * 1024),
            }

    def get_row_count(self):
        """Get total row count."""
        return self.spark.read.format("delta").load(self.table_path).count()

    def get_ticker_counts(self):
        """Get row counts per ticker."""
        return (
            self.spark.read.format("delta").load(self.table_path)
            .groupBy("ticker").count()
            .orderBy("ticker")
        )

    def get_history(self):
        """Get full table history."""
        return self.delta_table.history().select(
            "version", "timestamp", "operation", "operationMetrics"
        ).orderBy("version")

    def print_report(self, label=""):
        """Print a formatted health report."""
        stats = self.get_file_statistics()
        row_count = self.get_row_count()

        print(f"""
    {label} HEALTH REPORT
    {'=' * 50}
    Data Files:     {stats['num_files']}
    Total Size:     {stats['total_size_mb']:.4f} MB
    Avg File Size:  {stats['avg_size_mb']:.4f} MB
    Total Rows:     {row_count:,}
        """)
        return stats, row_count


def main():
    spark = get_spark()

    table_path = get_base_path("stocks_fixed")

    print("=" * 60)
    print("STOCK TABLE MAINTENANCE")
    print("=" * 60)

    # Verify the table exists
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
    except Exception as e:
        print(f"\nError: Fixed table not found at {table_path}")
        print("Run fixed_stock_harvester.py first!")
        stop_spark_if_local(spark)
        return

    health = StockTableHealthCheck(spark, table_path)

    # =========================================================================
    # BEFORE: Health Check
    # =========================================================================
    print("\n" + "-" * 60)
    print("BEFORE MAINTENANCE")
    print("-" * 60)

    before_stats, before_rows = health.print_report("BEFORE")

    print("Rows per ticker:")
    health.get_ticker_counts().show()

    # =========================================================================
    # 3a: OPTIMIZE with Z-ORDER
    # =========================================================================
    print("\n" + "-" * 60)
    print("3a: RUNNING OPTIMIZE (Z-ORDER on ticker, trade_date)")
    print("-" * 60)

    start = time.time()
    delta_table.optimize().executeZOrderBy("ticker", "trade_date")
    optimize_time = time.time() - start
    print(f"OPTIMIZE completed in {optimize_time:.2f} seconds")

    # =========================================================================
    # 3b: VACUUM
    # =========================================================================
    print("\n" + "-" * 60)
    print("3b: RUNNING VACUUM (0 hours retention — demo only, use 168+ in production)")
    print("-" * 60)

    start = time.time()
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 0 HOURS")
    vacuum_time = time.time() - start
    print(f"VACUUM completed in {vacuum_time:.2f} seconds")

    # =========================================================================
    # AFTER: Health Check
    # =========================================================================
    print("\n" + "-" * 60)
    print("AFTER MAINTENANCE")
    print("-" * 60)

    # Refresh the health checker after maintenance
    health = StockTableHealthCheck(spark, table_path)
    after_stats, after_rows = health.print_report("AFTER")

    print("Rows per ticker (verify no data lost):")
    health.get_ticker_counts().show()

    # =========================================================================
    # 3c: Before/After Comparison
    # =========================================================================
    print("\n" + "-" * 60)
    print("3c: BEFORE vs AFTER COMPARISON")
    print("-" * 60)

    print(f"""
    {'Metric':<25} {'Before':>15} {'After':>15} {'Change':>15}
    {'-' * 70}
    {'Data Files':<25} {before_stats['num_files']:>15} {after_stats['num_files']:>15} {after_stats['num_files'] - before_stats['num_files']:>+15}
    {'Total Size (MB)':<25} {before_stats['total_size_mb']:>15.4f} {after_stats['total_size_mb']:>15.4f} {after_stats['total_size_mb'] - before_stats['total_size_mb']:>+15.4f}
    {'Avg File Size (MB)':<25} {before_stats['avg_size_mb']:>15.4f} {after_stats['avg_size_mb']:>15.4f} {after_stats['avg_size_mb'] - before_stats['avg_size_mb']:>+15.4f}
    {'Total Rows':<25} {before_rows:>15,} {after_rows:>15,} {after_rows - before_rows:>+15,}
    """)

    if before_rows == after_rows:
        print("    Row count preserved — no data was lost!")
    else:
        print("    WARNING: Row count changed!")

    # =========================================================================
    # Table History
    # =========================================================================
    print("\n" + "-" * 60)
    print("TABLE HISTORY (all operations)")
    print("-" * 60)

    health.get_history().show(truncate=False)

    stop_spark_if_local(spark)
    print("\nMaintenance completed!")


if __name__ == "__main__":
    main()
