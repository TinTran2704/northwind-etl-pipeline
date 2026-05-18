"""
Tests for src/spark_jobs/clean_job.py

Covers all column property screens: not_null, max_length, numeric_range,
unique, date_range — and the FATAL escalation behaviour.

Uses local[2] SparkSession from conftest.py (no Docker required).
"""

import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.spark_jobs.clean_job import (
    CleanJobError,
    _apply_date_range_screen,
    _apply_max_length_screen,
    _apply_not_null_screen,
    _apply_numeric_range_screen,
    _apply_unique_screen,
    run_clean_job,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _customers_df(spark: SparkSession, rows: list[dict]):
    schema = T.StructType([
        T.StructField("CustomerID",  T.StringType(), True),
        T.StructField("CompanyName", T.StringType(), True),
        T.StructField("Country",     T.StringType(), True),
        T.StructField("PostalCode",  T.StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


def _orders_df(spark: SparkSession, rows: list[dict]):
    schema = T.StructType([
        T.StructField("OrderID",   T.StringType(), True),
        T.StructField("OrderDate", T.StringType(), True),
        T.StructField("Freight",   T.StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# not_null screen
# ---------------------------------------------------------------------------

class TestNotNullScreen:
    def test_null_pk_is_filtered(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": None,    "CompanyName": "X", "Country": "DE", "PostalCode": "12345"},
            {"CustomerID": "ALFKI", "CompanyName": "Y", "Country": "DE", "PostalCode": "12345"},
        ])
        good, errors = _apply_not_null_screen(df, "customers", "CustomerID", "FATAL", "b1", "CustomerID")
        assert good.count() == 1
        assert len(errors) == 1

    def test_no_nulls_passes_all_rows(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": "ALFKI", "CompanyName": "A", "Country": "DE", "PostalCode": "10"},
            {"CustomerID": "BONAP", "CompanyName": "B", "Country": "FR", "PostalCode": "20"},
        ])
        good, errors = _apply_not_null_screen(df, "customers", "CustomerID", "FATAL", "b2", "CustomerID")
        assert good.count() == 2
        assert errors == []

    def test_error_contains_pk_value_unknown_when_no_pk_col(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": None, "CompanyName": "X", "Country": "DE", "PostalCode": "1"},
        ])
        _, errors = _apply_not_null_screen(df, "customers", "CustomerID", "ERROR", "b3", pk_col=None)
        assert errors[0]["source_record_pk"] == "unknown"


# ---------------------------------------------------------------------------
# max_length screen
# ---------------------------------------------------------------------------

class TestMaxLengthScreen:
    def test_too_long_pk_filtered(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": "ALFKI",  "CompanyName": "A", "Country": "DE", "PostalCode": "1"},
            {"CustomerID": "TOOLONG", "CompanyName": "B", "Country": "DE", "PostalCode": "2"},
        ])
        good, errors = _apply_max_length_screen(df, "customers", "CustomerID", 5, "ERROR", "b4", "CustomerID")
        assert good.count() == 1
        good_ids = [r["CustomerID"] for r in good.collect()]
        assert "ALFKI" in good_ids

    def test_exact_max_length_passes(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": "ABCDE", "CompanyName": "A", "Country": "DE", "PostalCode": "1"},
        ])
        good, errors = _apply_max_length_screen(df, "customers", "CustomerID", 5, "ERROR", "b5", "CustomerID")
        assert good.count() == 1
        assert errors == []

    def test_missing_column_returns_unchanged_df(self, spark):
        df = _customers_df(spark, [{"CustomerID": "A", "CompanyName": "B", "Country": "C", "PostalCode": "D"}])
        good, errors = _apply_max_length_screen(df, "customers", "NONEXISTENT", 5, "WARN", "b6", "CustomerID")
        assert good.count() == 1
        assert errors == []


# ---------------------------------------------------------------------------
# numeric_range screen
# ---------------------------------------------------------------------------

class TestNumericRangeScreen:
    def test_out_of_range_filtered(self, spark):
        df = _orders_df(spark, [
            {"OrderID": "1", "OrderDate": "2023-01-01", "Freight": "-5"},
            {"OrderID": "2", "OrderDate": "2023-01-01", "Freight": "100"},
        ])
        good, errors = _apply_numeric_range_screen(df, "orders", "Freight", 0, 99999, "WARN", "b7", "OrderID")
        assert good.count() == 1
        assert len(errors) == 1
        assert errors[0]["column_name"] == "Freight"

    def test_null_value_passes_range_check(self, spark):
        df = _orders_df(spark, [
            {"OrderID": "3", "OrderDate": "2023-01-01", "Freight": None},
        ])
        good, errors = _apply_numeric_range_screen(df, "orders", "Freight", 0, 99999, "WARN", "b8", "OrderID")
        assert good.count() == 1
        assert errors == []

    def test_boundary_values_pass(self, spark):
        df = _orders_df(spark, [
            {"OrderID": "4", "OrderDate": "2023-01-01", "Freight": "0"},
            {"OrderID": "5", "OrderDate": "2023-01-01", "Freight": "99999"},
        ])
        good, errors = _apply_numeric_range_screen(df, "orders", "Freight", 0, 99999, "WARN", "b9", "OrderID")
        assert good.count() == 2
        assert errors == []


# ---------------------------------------------------------------------------
# unique screen
# ---------------------------------------------------------------------------

class TestUniqueScreen:
    def test_duplicate_pk_filtered(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": "DUP", "CompanyName": "A", "Country": "DE", "PostalCode": "1"},
            {"CustomerID": "DUP", "CompanyName": "B", "Country": "DE", "PostalCode": "2"},
            {"CustomerID": "UNI", "CompanyName": "C", "Country": "DE", "PostalCode": "3"},
        ])
        good, errors = _apply_unique_screen(df, "customers", "CustomerID", "FATAL", "b10", "CustomerID")
        assert good.count() == 2
        assert len(errors) == 1

    def test_all_unique_passes(self, spark):
        df = _customers_df(spark, [
            {"CustomerID": "A", "CompanyName": "A", "Country": "DE", "PostalCode": "1"},
            {"CustomerID": "B", "CompanyName": "B", "Country": "DE", "PostalCode": "2"},
        ])
        good, errors = _apply_unique_screen(df, "customers", "CustomerID", "FATAL", "b11", "CustomerID")
        assert good.count() == 2
        assert errors == []


# ---------------------------------------------------------------------------
# date_range screen
# ---------------------------------------------------------------------------

class TestDateRangeScreen:
    def test_old_date_filtered(self, spark):
        df = _orders_df(spark, [
            {"OrderID": "1", "OrderDate": "1800-01-01", "Freight": "10"},
            {"OrderID": "2", "OrderDate": "2023-06-15", "Freight": "10"},
        ])
        good, errors = _apply_date_range_screen(
            df, "orders", "OrderDate", "1990-01-01", "today", "ERROR", "b12", "OrderID"
        )
        assert good.count() == 1
        assert len(errors) == 1

    def test_today_max_date_accepts_recent_date(self, spark):
        df = _orders_df(spark, [
            {"OrderID": "1", "OrderDate": "2024-12-01", "Freight": "10"},
        ])
        good, errors = _apply_date_range_screen(
            df, "orders", "OrderDate", "1990-01-01", "today", "ERROR", "b13", "OrderID"
        )
        assert good.count() == 1
        assert errors == []


# ---------------------------------------------------------------------------
# run_clean_job integration test (local filesystem, no Postgres)
# ---------------------------------------------------------------------------

class TestRunCleanJob:
    def test_clean_job_filters_null_pk_and_writes_parquet(self, spark, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        staging_dir = tmp_path / "staging"

        # Write a customers CSV with one null PK (FATAL) — but severity is ERROR here
        # so batch should NOT stop; we override via a minimal config
        customers_csv = raw_dir / "customers.csv"
        customers_csv.write_text(
            "CustomerID,CompanyName,Country,PostalCode\n"
            "ALFKI,Alfreds,Germany,12209\n"
            ",NoName,Germany,00000\n"
            "BONAP,Bon app,France,13008\n"
        )

        # Minimal config with CompanyName not_null as ERROR (not FATAL) so batch continues
        config = tmp_path / "rules.yaml"
        config.write_text(
            "screens:\n"
            "  customers:\n"
            "    column_property:\n"
            "      - {column: CustomerID, rule: not_null, severity: ERROR}\n"
        )

        counts = run_clean_job(
            batch_id="test-001",
            raw_dir=str(raw_dir),
            staging_dir=str(staging_dir),
            config_path=str(config),
            write_error_events=False,
        )

        assert "customers" in counts
        assert counts["customers"] == 2  # null row filtered out

        import os
        out_path = staging_dir / "cleaned_customers"
        assert out_path.exists()

    def test_clean_job_fatal_violation_stops_batch(self, spark, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "customers.csv").write_text(
            "CustomerID,CompanyName,Country,PostalCode\n"
            ",NoName,Germany,00000\n"
        )
        config = tmp_path / "rules.yaml"
        config.write_text(
            "screens:\n"
            "  customers:\n"
            "    column_property:\n"
            "      - {column: CustomerID, rule: not_null, severity: FATAL}\n"
        )
        with pytest.raises(CleanJobError, match="FATAL"):
            run_clean_job(
                batch_id="test-fatal",
                raw_dir=str(raw_dir),
                staging_dir=str(tmp_path / "staging"),
                config_path=str(config),
                write_error_events=False,
            )
