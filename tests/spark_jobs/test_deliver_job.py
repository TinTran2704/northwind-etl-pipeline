"""
Tests for src/spark_jobs/deliver_job.py

Covers SK generation, point-in-time resolution, unknown member fallback,
and fact_sales derived measures.

Uses local[2] SparkSession from conftest.py (no Docker/Postgres required).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.spark_jobs.deliver_job import (
    _UNKNOWN_SK,
    _assign_surrogate_keys,
    _resolve_sk_point_in_time,
    _build_fact_sales,
    DeliverJobError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dim_customer(spark: SparkSession):
    schema = T.StructType([
        T.StructField("customer_sk",     T.LongType(),   False),
        T.StructField("customer_nk",     T.StringType(), True),
        T.StructField("company_name",    T.StringType(), True),
        T.StructField("effective_date",  T.StringType(), True),
        T.StructField("expiration_date", T.StringType(), True),
        T.StructField("is_current",      T.BooleanType(), True),
    ])
    rows = [
        (1, "ALFKI", "Alfreds", "2000-01-01", None,         True),
        (2, "BONAP", "Bon app", "2000-01-01", "2020-12-31", False),
        (3, "BONAP", "Bon app v2", "2021-01-01", None,      True),
    ]
    return spark.createDataFrame(rows, schema=schema)


def _dim_product(spark: SparkSession):
    schema = T.StructType([
        T.StructField("product_sk", T.LongType(),   False),
        T.StructField("product_nk", T.LongType(),   True),
        T.StructField("product_name", T.StringType(), True),
    ])
    return spark.createDataFrame([(10, 1, "Chai"), (11, 2, "Chang")], schema=schema)


def _orders_parquet(spark, tmp_path):
    schema = T.StructType([
        T.StructField("orderID",       T.StringType(), True),
        T.StructField("customerID",    T.StringType(), True),
        T.StructField("employeeID",    T.StringType(), True),
        T.StructField("orderDate",     T.StringType(), True),
        T.StructField("requiredDate",  T.StringType(), True),
        T.StructField("shippedDate",   T.StringType(), True),
        T.StructField("shipVia",       T.StringType(), True),
        T.StructField("freight",       T.StringType(), True),
        T.StructField("shipCountry",   T.StringType(), True),
    ])
    rows = [
        ("10248", "ALFKI", "5", "1996-07-04", "1996-08-01", "1996-07-16", "3", "32.38", "France"),
        ("10249", "BONAP", "6", "2022-06-15", "2022-07-13", None,          "1", "11.61", "Germany"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    out = tmp_path / "cleaned_orders"
    df.write.parquet(str(out))
    return out


def _details_parquet(spark, tmp_path):
    schema = T.StructType([
        T.StructField("orderID",   T.StringType(), True),
        T.StructField("productID", T.StringType(), True),
        T.StructField("unitPrice", T.StringType(), True),
        T.StructField("quantity",  T.StringType(), True),
        T.StructField("discount",  T.StringType(), True),
    ])
    rows = [
        ("10248", "1", "14.40", "12",  "0.0"),
        ("10249", "2", "9.80",  "10",  "0.0"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    out = tmp_path / "cleaned_order_details"
    df.write.parquet(str(out))
    return out


# ---------------------------------------------------------------------------
# SK generation
# ---------------------------------------------------------------------------

class TestAssignSurrogateKeys:
    def test_sk_starts_at_offset_plus_one(self, spark):
        schema = T.StructType([T.StructField("name", T.StringType(), True)])
        df = spark.createDataFrame([("a",), ("b",), ("c",)], schema=schema)
        result = _assign_surrogate_keys(df, "dim_test", "test_sk", offset=100)
        sks = sorted([r["test_sk"] for r in result.collect()])
        assert sks == [101, 102, 103]

    def test_sk_offset_zero_starts_at_one(self, spark):
        schema = T.StructType([T.StructField("val", T.StringType(), True)])
        df = spark.createDataFrame([("x",)], schema=schema)
        result = _assign_surrogate_keys(df, "dim_test", "sk", offset=0)
        assert result.collect()[0]["sk"] == 1

    def test_empty_df_returns_df_with_unknown_sk(self, spark):
        schema = T.StructType([T.StructField("val", T.StringType(), True)])
        df = spark.createDataFrame([], schema=schema)
        result = _assign_surrogate_keys(df, "dim_test", "sk", offset=5)
        assert result.count() == 0


# ---------------------------------------------------------------------------
# Point-in-time SK resolution
# ---------------------------------------------------------------------------

class TestResolveSkPointInTime:
    def test_resolves_correct_version_by_date(self, spark):
        """BONAP in 2019 should resolve to sk=2 (old row), not sk=3 (new row)."""
        schema = T.StructType([
            T.StructField("orderID",    T.StringType(), True),
            T.StructField("customerID", T.StringType(), True),
            T.StructField("orderDate",  T.StringType(), True),
        ])
        fact = spark.createDataFrame(
            [("10248", "BONAP", "2019-06-15")],
            schema=schema,
        )
        dim = _dim_customer(spark)
        result = _resolve_sk_point_in_time(
            fact, dim, "customerID", "orderDate",
            "customer_nk", "customer_sk", "customer_sk", has_scd2=True,
        )
        row = result.collect()[0]
        assert row["customer_sk"] == 2

    def test_resolves_current_version_for_open_ended_row(self, spark):
        """ALFKI has no expiration — any date should resolve to sk=1."""
        schema = T.StructType([
            T.StructField("orderID",    T.StringType(), True),
            T.StructField("customerID", T.StringType(), True),
            T.StructField("orderDate",  T.StringType(), True),
        ])
        fact = spark.createDataFrame([("1", "ALFKI", "2024-01-01")], schema=schema)
        dim = _dim_customer(spark)
        result = _resolve_sk_point_in_time(
            fact, dim, "customerID", "orderDate",
            "customer_nk", "customer_sk", "customer_sk", has_scd2=True,
        )
        assert result.collect()[0]["customer_sk"] == 1

    def test_unknown_nk_returns_unknown_sk(self, spark):
        """An nk that doesn't exist in the dim should return -1."""
        schema = T.StructType([
            T.StructField("orderID",    T.StringType(), True),
            T.StructField("customerID", T.StringType(), True),
            T.StructField("orderDate",  T.StringType(), True),
        ])
        fact = spark.createDataFrame([("1", "XXXX", "2023-01-01")], schema=schema)
        dim = _dim_customer(spark)
        result = _resolve_sk_point_in_time(
            fact, dim, "customerID", "orderDate",
            "customer_nk", "customer_sk", "customer_sk", has_scd2=True,
        )
        assert result.collect()[0]["customer_sk"] == _UNKNOWN_SK

    def test_none_dim_returns_unknown_sk(self, spark):
        """When dim is None (not loaded), every row gets -1."""
        schema = T.StructType([
            T.StructField("orderID",    T.StringType(), True),
            T.StructField("customerID", T.StringType(), True),
            T.StructField("orderDate",  T.StringType(), True),
        ])
        fact = spark.createDataFrame([("1", "ALFKI", "2023-01-01")], schema=schema)
        result = _resolve_sk_point_in_time(
            fact, None, "customerID", "orderDate",
            "customer_nk", "customer_sk", "customer_sk", has_scd2=True,
        )
        assert result.collect()[0]["customer_sk"] == _UNKNOWN_SK


# ---------------------------------------------------------------------------
# _build_fact_sales
# ---------------------------------------------------------------------------

class TestBuildFactSales:
    def test_fact_row_count_matches_order_details(self, spark, tmp_path):
        _orders_parquet(spark, tmp_path)
        _details_parquet(spark, tmp_path)
        dims = {
            "dim_customer": _dim_customer(spark),
            "dim_product":  _dim_product(spark),
        }
        fact = _build_fact_sales(spark, str(tmp_path), dims, batch_id="test-fact-001")
        # 2 order-detail lines → 2 fact rows
        assert fact.count() == 2

    def test_extended_price_computed_correctly(self, spark, tmp_path):
        _orders_parquet(spark, tmp_path)
        _details_parquet(spark, tmp_path)
        dims = {}
        fact = _build_fact_sales(spark, str(tmp_path), dims, batch_id="test-fact-002")
        rows = {r["order_id"]: r for r in fact.collect()}
        # order 10248: qty=12, price=14.40 → extended=172.80
        assert abs(rows[10248]["extended_price"] - 172.80) < 0.01

    def test_freight_allocation_sums_to_order_freight(self, spark, tmp_path):
        _orders_parquet(spark, tmp_path)
        _details_parquet(spark, tmp_path)
        dims = {}
        fact = _build_fact_sales(spark, str(tmp_path), dims, batch_id="test-fact-003")
        # Each order has 1 line → freight_allocated == freight
        rows = {r["order_id"]: r for r in fact.collect()}
        assert abs(rows[10248]["freight_allocated"] - 32.38) < 0.01

    def test_missing_parquet_raises_deliver_job_error(self, spark, tmp_path):
        with pytest.raises(DeliverJobError):
            _build_fact_sales(spark, str(tmp_path), {}, batch_id="test-err")
