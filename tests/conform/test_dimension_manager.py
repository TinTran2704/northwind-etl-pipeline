"""Tests for src/conform/dimension_manager.py."""

import pandas as pd
import pytest

from src.conform.dimension_manager import DimensionManager, DimensionManagerError


def _mgr(tmp_path) -> DimensionManager:
    return DimensionManager(base_dir=tmp_path / "published")


def _df(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "country_code":  ["DE", "FR", "US"][:n],
        "country_name":  ["Germany", "France", "United States"][:n],
        "region":        ["Europe", "Europe", "Americas"][:n],
    })


class TestPublish:
    def test_publish_creates_latest_parquet(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(), "batch-001")
        assert (tmp_path / "published" / "dim_geography" / "latest.parquet").exists()

    def test_publish_creates_versioned_parquet(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(), "batch-001")
        assert (tmp_path / "published" / "dim_geography" / "batch-001.parquet").exists()

    def test_publish_returns_batch_id(self, tmp_path):
        mgr = _mgr(tmp_path)
        version = mgr.publish("dim_geography", _df(), "batch-abc")
        assert version == "batch-abc"

    def test_publish_empty_df_raises(self, tmp_path):
        mgr = _mgr(tmp_path)
        with pytest.raises(DimensionManagerError):
            mgr.publish("dim_geography", pd.DataFrame(), "batch-001")

    def test_latest_overwritten_on_second_publish(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(3), "batch-001")
        mgr.publish("dim_geography", _df(1), "batch-002")
        latest = pd.read_parquet(tmp_path / "published" / "dim_geography" / "latest.parquet")
        assert len(latest) == 1

    def test_versioned_file_not_overwritten(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(3), "batch-001")
        mgr.publish("dim_geography", _df(1), "batch-002")
        v1 = pd.read_parquet(tmp_path / "published" / "dim_geography" / "batch-001.parquet")
        assert len(v1) == 3

    def test_multiple_dimensions_independent(self, tmp_path):
        mgr = _mgr(tmp_path)
        df_geo = _df(3)
        df_cust = pd.DataFrame({"customerID": ["A"], "companyName": ["Corp"]})
        mgr.publish("dim_geography", df_geo, "b1")
        mgr.publish("dim_customer", df_cust, "b1")
        assert (tmp_path / "published" / "dim_geography" / "latest.parquet").exists()
        assert (tmp_path / "published" / "dim_customer" / "latest.parquet").exists()


class TestGetLatest:
    def test_returns_published_dataframe(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(3), "batch-001")
        result = mgr.get_latest("dim_geography")
        assert len(result) == 3
        assert "country_code" in result.columns

    def test_raises_when_not_published(self, tmp_path):
        mgr = _mgr(tmp_path)
        with pytest.raises(DimensionManagerError):
            mgr.get_latest("dim_nonexistent")

    def test_get_latest_after_two_publishes_returns_newest(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(3), "batch-001")
        mgr.publish("dim_geography", _df(1), "batch-002")
        result = mgr.get_latest("dim_geography")
        assert len(result) == 1


class TestGetVersion:
    def test_returns_correct_version(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(3), "batch-001")
        mgr.publish("dim_geography", _df(1), "batch-002")
        v1 = mgr.get_version("dim_geography", "batch-001")
        assert len(v1) == 3

    def test_raises_on_unknown_version(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(), "batch-001")
        with pytest.raises(DimensionManagerError):
            mgr.get_version("dim_geography", "batch-999")


class TestListVersions:
    def test_empty_when_no_publishes(self, tmp_path):
        mgr = _mgr(tmp_path)
        assert mgr.list_versions("dim_geography") == []

    def test_lists_all_batch_ids(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(), "batch-001")
        mgr.publish("dim_geography", _df(), "batch-002")
        versions = mgr.list_versions("dim_geography")
        assert "batch-001" in versions
        assert "batch-002" in versions
        assert "latest" not in versions

    def test_sorted_ascending(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.publish("dim_geography", _df(), "2024-01")
        mgr.publish("dim_geography", _df(), "2024-03")
        mgr.publish("dim_geography", _df(), "2024-02")
        versions = mgr.list_versions("dim_geography")
        assert versions == sorted(versions)
