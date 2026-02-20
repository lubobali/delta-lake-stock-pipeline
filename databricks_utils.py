"""
Databricks Compatibility Utilities
===================================

Shared utility module for the stock data pipeline.
Works in both local PySpark and Databricks environments.
"""

import os


def is_databricks():
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DB_HOME" in os.environ
        or os.path.exists("/databricks")
    )


def get_spark():
    if is_databricks():
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .master("local[*]")
        )

        return configure_spark_with_delta_pip(builder).getOrCreate()


def get_base_path(subdir=""):
    if is_databricks():
        base = "/Volumes/tabular/default/delta_stock_pipeline"
    else:
        base = "./delta_tables"
        os.makedirs(base, exist_ok=True)

    if subdir:
        path = f"{base}/{subdir}"
        if not is_databricks():
            os.makedirs(path, exist_ok=True)
        return path
    return base


def cleanup_path(spark, path):
    if is_databricks():
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            dbutils.fs.rm(path, recurse=True)
        except Exception:
            try:
                spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
            except Exception:
                pass
    else:
        import shutil
        if os.path.exists(path):
            shutil.rmtree(path)


def stop_spark_if_local(spark):
    if not is_databricks():
        spark.stop()
