"""Tests for src/conform/survivor_selector.py."""

import pandas as pd
import pytest
import yaml

from src.conform.survivor_selector import SurvivorSelector


def _selector(tmp_path, rules: dict | None = None) -> SurvivorSelector:
    cfg = tmp_path / "survivorship_rules.yaml"
    default_rules = {
        "customers": {
            "companyName": {"rule": "longest_non_null"},
            "phone":       {"rule": "longest_non_null"},
            "city":        {"rule": "longest_non_null"},
        },
        "products": {
            "unitPrice": {"rule": "most_recent"},
        },
        "default": {"_default": {"rule": "longest_non_null"}},
    }
    cfg.write_text(yaml.dump(rules or default_rules))
    return SurvivorSelector(config_path=cfg)


def _cluster_df(*rows, cluster_ids=None) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if cluster_ids:
        df.insert(0, "cluster_id", cluster_ids)
    return df


class TestSingleMemberCluster:
    def test_single_member_returned_as_is(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "ALFKI", "companyName": "Alfreds", "city": "Berlin", "phone": "030"}
        ])
        result = s.select(df, "customers")
        assert len(result) == 1
        assert result.iloc[0]["customerID"] == "ALFKI"

    def test_all_singletons_pass_through(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "ALFKI", "companyName": "A", "city": "Berlin", "phone": "030"},
            {"cluster_id": "c002", "customerID": "ANATR", "companyName": "B", "city": "Mexico", "phone": "555"},
        ])
        result = s.select(df, "customers")
        assert len(result) == 2


class TestLongestNonNull:
    def test_picks_longer_company_name(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "ALFKI",     "companyName": "Alfreds",              "city": "Berlin", "phone": "030"},
            {"cluster_id": "c001", "customerID": "ALFKI_DUP", "companyName": "Alfreds Futterkiste",  "city": "Berlin", "phone": "030-007"},
        ])
        result = s.select(df, "customers")
        assert len(result) == 1
        assert result.iloc[0]["companyName"] == "Alfreds Futterkiste"

    def test_null_value_not_selected(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": None,    "city": "Paris", "phone": "01"},
            {"cluster_id": "c001", "customerID": "B", "companyName": "Bonap", "city": "Paris", "phone": "01"},
        ])
        result = s.select(df, "customers")
        assert result.iloc[0]["companyName"] == "Bonap"

    def test_all_null_returns_none(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": None, "city": "Paris", "phone": "01"},
            {"cluster_id": "c001", "customerID": "B", "companyName": None, "city": "Paris", "phone": "01"},
        ])
        result = s.select(df, "customers")
        assert result.iloc[0]["companyName"] is None


class TestMostRecent:
    def test_picks_value_from_most_recent_row(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "productID": 1, "unitPrice": 9.99, "_extract_ts": "2024-01-01"},
            {"cluster_id": "c001", "productID": 1, "unitPrice": 12.50, "_extract_ts": "2024-06-01"},
        ])
        result = s.select(df, "products")
        assert result.iloc[0]["unitPrice"] == 12.50

    def test_falls_back_to_first_non_null_without_ts(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "productID": 1, "unitPrice": 9.99},
            {"cluster_id": "c001", "productID": 1, "unitPrice": 15.00},
        ])
        result = s.select(df, "products")
        assert result.iloc[0]["unitPrice"] in (9.99, 15.00)


class TestPreferSource:
    def test_prefers_configured_source(self, tmp_path):
        rules = {
            "customers": {
                "phone": {"rule": "prefer_source", "priority": ["northwind", "crm"]},
                "companyName": {"rule": "longest_non_null"},
                "city": {"rule": "longest_non_null"},
            },
            "default": {"_default": {"rule": "longest_non_null"}},
        }
        s = _selector(tmp_path, rules)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": "X", "city": "Y", "phone": "111", "_source": "crm"},
            {"cluster_id": "c001", "customerID": "B", "companyName": "X", "city": "Y", "phone": "999", "_source": "northwind"},
        ])
        result = s.select(df, "customers")
        assert result.iloc[0]["phone"] == "999"  # northwind preferred

    def test_falls_back_when_source_missing(self, tmp_path):
        rules = {
            "customers": {
                "phone": {"rule": "prefer_source", "priority": ["northwind"]},
                "companyName": {"rule": "longest_non_null"},
                "city": {"rule": "longest_non_null"},
            },
            "default": {"_default": {"rule": "longest_non_null"}},
        }
        s = _selector(tmp_path, rules)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": "X", "city": "Y", "phone": "555"},
            {"cluster_id": "c001", "customerID": "B", "companyName": "X", "city": "Y", "phone": "666"},
        ])
        result = s.select(df, "customers")
        # No _source column — fallback to first non-null
        assert result.iloc[0]["phone"] in ("555", "666")


class TestOutputShape:
    def test_output_has_same_columns_as_input(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": "X", "city": "Y", "phone": "1"},
            {"cluster_id": "c002", "customerID": "B", "companyName": "Z", "city": "W", "phone": "2"},
        ])
        result = s.select(df, "customers")
        assert list(result.columns) == list(df.columns)

    def test_one_golden_record_per_cluster(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": "X", "city": "Y", "phone": "1"},
            {"cluster_id": "c001", "customerID": "B", "companyName": "XX", "city": "Y", "phone": "2"},
            {"cluster_id": "c002", "customerID": "C", "companyName": "Z", "city": "W", "phone": "3"},
        ])
        result = s.select(df, "customers")
        assert len(result) == 2
        assert result["cluster_id"].nunique() == 2

    def test_missing_cluster_id_raises(self, tmp_path):
        s = _selector(tmp_path)
        df = pd.DataFrame([{"customerID": "A", "companyName": "X"}])
        with pytest.raises(Exception):
            s.select(df, "customers")

    def test_missing_config_file_uses_default_rule(self, tmp_path):
        s = SurvivorSelector(config_path=tmp_path / "nonexistent.yaml")
        df = pd.DataFrame([
            {"cluster_id": "c001", "customerID": "A", "companyName": "X"},
            {"cluster_id": "c001", "customerID": "B", "companyName": "XY"},
        ])
        result = s.select(df, "customers")
        assert len(result) == 1
        assert result.iloc[0]["companyName"] == "XY"
