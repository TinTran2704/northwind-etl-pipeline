"""Shared SparkSession fixture for all spark_jobs tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local SparkSession for unit tests — no external cluster needed."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ETL-Test")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
