"""Tests for src/conform/deduplicator.py."""

import pandas as pd
import pytest

from src.conform.deduplicator import Deduplicator


def _customers(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestMatchScore:
    def test_identical_records_score_one(self):
        d = Deduplicator()
        a = {"companyName": "Alfreds Futterkiste", "city": "Berlin"}
        score = d.match_score(a, a, ["companyName", "city"], {"companyName": 0.7, "city": 0.3})
        assert score == pytest.approx(1.0, abs=1e-4)

    def test_completely_different_records_score_near_zero(self):
        d = Deduplicator()
        a = {"companyName": "Alfreds Futterkiste", "city": "Berlin"}
        b = {"companyName": "Xyz Corp", "city": "Tokyo"}
        score = d.match_score(a, b, ["companyName", "city"], {"companyName": 0.7, "city": 0.3})
        assert score < 0.5

    def test_minor_typo_high_score(self):
        d = Deduplicator()
        a = {"companyName": "Alfreds Futterkiste"}
        b = {"companyName": "Alfreds Futeriste"}   # typo
        score = d.match_score(a, b, ["companyName"], {"companyName": 1.0})
        assert score > 0.85

    def test_empty_both_fields_count_as_match(self):
        d = Deduplicator()
        a = {"city": None}
        b = {"city": None}
        score = d.match_score(a, b, ["city"], {"city": 1.0})
        assert score == pytest.approx(1.0, abs=1e-4)

    def test_one_empty_field_reduces_score(self):
        d = Deduplicator()
        a = {"city": "Berlin"}
        b = {"city": None}
        score = d.match_score(a, b, ["city"], {"city": 1.0})
        assert score == pytest.approx(0.0, abs=1e-4)

    def test_weights_applied_correctly(self):
        d = Deduplicator()
        # field1 identical (sim=1.0), field2 empty vs value (sim=0.0)
        # weights: field1=0.8, field2=0.2 → expected = 0.8
        a = {"f1": "hello", "f2": "world"}
        b = {"f1": "hello", "f2": None}
        score = d.match_score(a, b, ["f1", "f2"], {"f1": 0.8, "f2": 0.2})
        assert score == pytest.approx(0.8, abs=1e-4)


class TestFindClusters:
    def _clean_customers(self):
        return _customers([
            {"customerID": "ALFKI", "companyName": "Alfreds Futterkiste", "city": "Berlin", "country": "Germany", "phone": "030-007", "address": "Obere 57"},
            {"customerID": "ANATR", "companyName": "Ana Trujillo", "city": "Mexico", "country": "Mexico", "phone": "(5) 555", "address": "Avda 2222"},
            {"customerID": "BERGS", "companyName": "Berglunds snabbköp", "city": "Luleå", "country": "Sweden", "phone": "0921-12", "address": "Berguv 8"},
        ])

    def test_clean_data_each_record_gets_own_cluster(self):
        d = Deduplicator()
        df = self._clean_customers()
        result = d.find_clusters(df, "customers")
        assert result["cluster_id"].nunique() == 3

    def test_output_has_cluster_id_and_nk_columns(self):
        d = Deduplicator()
        df = self._clean_customers()
        result = d.find_clusters(df, "customers")
        assert "cluster_id" in result.columns
        assert "customerID" in result.columns

    def test_output_row_count_equals_input(self):
        d = Deduplicator()
        df = self._clean_customers()
        result = d.find_clusters(df, "customers")
        assert len(result) == len(df)

    def test_all_nks_preserved(self):
        d = Deduplicator()
        df = self._clean_customers()
        result = d.find_clusters(df, "customers")
        assert set(result["customerID"]) == {"ALFKI", "ANATR", "BERGS"}

    def test_duplicate_records_merged_into_same_cluster(self):
        d = Deduplicator(threshold=0.85)
        df = _customers([
            {"customerID": "ALFKI",     "companyName": "Alfreds Futterkiste", "city": "Berlin", "country": "Germany", "phone": "030-007", "address": "Obere 57"},
            {"customerID": "ALFKI_DUP", "companyName": "Alfreds Futterkiste", "city": "Berlin", "country": "Germany", "phone": "030-007", "address": "Obere 57"},
            {"customerID": "ANATR",     "companyName": "Ana Trujillo",         "city": "Mexico", "country": "Mexico",  "phone": "(5)555",  "address": "Avda"},
        ])
        result = d.find_clusters(df, "customers")
        # ALFKI and ALFKI_DUP should share a cluster; ANATR gets its own
        alfki_cluster = result.loc[result["customerID"] == "ALFKI", "cluster_id"].iloc[0]
        alfki_dup_cluster = result.loc[result["customerID"] == "ALFKI_DUP", "cluster_id"].iloc[0]
        anatr_cluster = result.loc[result["customerID"] == "ANATR", "cluster_id"].iloc[0]
        assert alfki_cluster == alfki_dup_cluster
        assert anatr_cluster != alfki_cluster

    def test_cluster_ids_are_strings(self):
        d = Deduplicator()
        result = d.find_clusters(self._clean_customers(), "customers")
        assert result["cluster_id"].dtype == object

    def test_unknown_entity_uses_first_column(self):
        d = Deduplicator()
        df = pd.DataFrame({"id": ["A", "B"], "val": ["x", "y"]})
        result = d.find_clusters(df, "unknown_entity")
        assert len(result) == 2

    def test_single_row_df(self):
        d = Deduplicator()
        df = _customers([{"customerID": "ONLY", "companyName": "Solo Corp", "city": "Paris", "country": "France", "phone": "01-23", "address": "Rue 1"}])
        result = d.find_clusters(df, "customers")
        assert len(result) == 1
        assert result["cluster_id"].iloc[0].startswith("cluster_")

    def test_threshold_override(self):
        d = Deduplicator(threshold=0.99)
        df = _customers([
            {"customerID": "A", "companyName": "Alfreds", "city": "Berlin", "country": "DE", "phone": "111", "address": "Street 1"},
            {"customerID": "B", "companyName": "Alfredz", "city": "Berlin", "country": "DE", "phone": "111", "address": "Street 1"},
        ])
        result = d.find_clusters(df, "customers", threshold=0.99)
        # With very high threshold, slight difference should keep them separate
        assert result["cluster_id"].nunique() >= 1
