"""
Spark session factory and JDBC helpers shared across all Spark jobs.

Detects whether running inside Docker (spark://spark-master:7077)
or locally (local[2]) via the SPARK_MASTER env var.
"""

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_JDBC_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"
_DEFAULT_MASTER = "local[2]"


def get_spark_session(app_name: str) -> SparkSession:
    """Build and return a SparkSession.

    Uses SPARK_MASTER env var when set (Docker: spark://spark-master:7077),
    falls back to local[2] for unit tests and local runs.

    Args:
        app_name: Logical application name shown in Spark UI.

    Returns:
        Configured SparkSession.
    """
    master = os.environ.get("SPARK_MASTER", _DEFAULT_MASTER)
    logger.info("[SPARK] Starting SparkSession app=%s master=%s", app_name, master)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Only add JDBC jar when the path actually exists (skipped in unit tests)
    if os.path.exists(_JDBC_JAR):
        builder = builder.config("spark.jars", _JDBC_JAR)

    return builder.getOrCreate()


def get_pg_jdbc_url() -> str:
    """Build PostgreSQL JDBC URL from environment variables.

    Expected env vars: ETL_DW_HOST, ETL_DW_PORT, ETL_DW_DATABASE.

    Returns:
        JDBC connection URL string.
    """
    host = os.environ.get("ETL_DW_HOST", "localhost")
    port = os.environ.get("ETL_DW_PORT", "5432")
    database = os.environ.get("ETL_DW_DATABASE", "northwind_dw")
    return f"jdbc:postgresql://{host}:{port}/{database}"


def get_pg_jdbc_properties() -> dict:
    """Build JDBC connection properties dict from environment variables.

    Expected env vars: ETL_DW_USER, ETL_DW_PASSWORD.

    Returns:
        Dict suitable for spark.read.jdbc / df.write.jdbc properties arg.
    """
    return {
        "user": os.environ.get("ETL_DW_USER", "etl_user"),
        "password": os.environ.get("ETL_DW_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }
