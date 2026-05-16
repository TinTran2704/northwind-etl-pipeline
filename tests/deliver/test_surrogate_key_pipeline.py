"""Tests for SurrogateKeyPipeline (Kimball Subsystem #14)."""

from datetime import date

import pandas as pd
import pytest

from src.deliver.surrogate_key_pipeline import LookupConfig, SKPipelineError, SurrogateKeyPipeline


@pytest.fixture
def pipeline():
    return SurrogateKeyPipeline()


def _make_dim(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestDateToSk:
    def test_string_date(self, pipeline):
        assert SurrogateKeyPipeline.date_to_sk("1996-07-04") == 19960704

    def test_date_object(self, pipeline):
        assert SurrogateKeyPipeline.date_to_sk(date(2024, 6, 15)) == 20240615

    def test_timestamp(self, pipeline):
        ts = pd.Timestamp("2020-01-01")
        assert SurrogateKeyPipeline.date_to_sk(ts) == 20200101

    def test_invalid_returns_unknown(self, pipeline):
        assert SurrogateKeyPipeline.date_to_sk("not-a-date") == 19000101

    def test_none_returns_unknown(self, pipeline):
        assert SurrogateKeyPipeline.date_to_sk(None) == 19000101

    def test_nat_returns_unknown(self, pipeline):
        assert SurrogateKeyPipeline.date_to_sk(pd.NaT) == 19000101


class TestResolveSk:
    def _dim(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"customer_nk": "ALFKI", "customer_sk": 1,
             "effective_date": "2020-01-01", "expiration_date": "2022-12-31", "is_current": False},
            {"customer_nk": "ALFKI", "customer_sk": 2,
             "effective_date": "2023-01-01", "expiration_date": None, "is_current": True},
        ])

    def test_point_in_time_returns_correct_sk(self, pipeline):
        dim = self._dim()
        sk = pipeline.resolve_sk("ALFKI", "2021-06-01", dim, "customer_nk", "customer_sk")
        assert sk == 1

    def test_current_row_returned_for_recent_date(self, pipeline):
        dim = self._dim()
        sk = pipeline.resolve_sk("ALFKI", "2024-01-01", dim, "customer_nk", "customer_sk")
        assert sk == 2

    def test_unknown_nk_returns_minus_one(self, pipeline):
        dim = self._dim()
        sk = pipeline.resolve_sk("GHOST", "2024-01-01", dim, "customer_nk", "customer_sk")
        assert sk == -1

    def test_none_nk_returns_minus_one(self, pipeline):
        dim = self._dim()
        sk = pipeline.resolve_sk(None, "2024-01-01", dim, "customer_nk", "customer_sk")
        assert sk == -1

    def test_no_eff_exp_columns_falls_back_to_nk_match(self, pipeline):
        dim = pd.DataFrame([{"product_nk": 1, "product_sk": 10}])
        sk = pipeline.resolve_sk(1, None, dim, "product_nk", "product_sk")
        assert sk == 10


class TestResolveType1Sk:
    def test_returns_correct_sk(self, pipeline):
        dim = pd.DataFrame([{"shipper_nk": 3, "shipper_sk": 100}])
        assert pipeline.resolve_type1_sk(3, dim, "shipper_nk", "shipper_sk") == 100

    def test_missing_nk_returns_minus_one(self, pipeline):
        dim = pd.DataFrame([{"shipper_nk": 3, "shipper_sk": 100}])
        assert pipeline.resolve_type1_sk(99, dim, "shipper_nk", "shipper_sk") == -1

    def test_none_returns_minus_one(self, pipeline):
        dim = pd.DataFrame([{"shipper_nk": 3, "shipper_sk": 100}])
        assert pipeline.resolve_type1_sk(None, dim, "shipper_nk", "shipper_sk") == -1


class TestResolveBatch:
    def test_resolves_multiple_lookups(self, pipeline):
        fact = pd.DataFrame([
            {"customerID": "ALFKI", "shipVia": 2, "order_date": pd.Timestamp("2024-01-01")},
        ])
        cust_dim = pd.DataFrame([{
            "customer_nk": "ALFKI", "customer_sk": 5,
            "effective_date": "2020-01-01", "expiration_date": None,
        }])
        ship_dim = pd.DataFrame([{"shipper_nk": 2, "shipper_sk": 20}])

        lookups = [
            LookupConfig("customerID", cust_dim, "customer_nk", "customer_sk",
                         "customer_sk", date_col="order_date"),
            LookupConfig("shipVia", ship_dim, "shipper_nk", "shipper_sk", "shipper_sk"),
        ]
        result = pipeline.resolve_batch(fact, lookups)
        assert result.iloc[0]["customer_sk"] == 5
        assert result.iloc[0]["shipper_sk"] == 20

    def test_missing_source_col_fills_unknown(self, pipeline):
        fact = pd.DataFrame([{"some_col": "x"}])
        dim = pd.DataFrame([{"customer_nk": "ALFKI", "customer_sk": 5}])
        lookups = [LookupConfig("customerID", dim, "customer_nk", "customer_sk", "customer_sk")]
        result = pipeline.resolve_batch(fact, lookups)
        assert result.iloc[0]["customer_sk"] == -1

    def test_original_fact_df_not_mutated(self, pipeline):
        fact = pd.DataFrame([{"customerID": "ALFKI"}])
        dim = pd.DataFrame([{"customer_nk": "ALFKI", "customer_sk": 5}])
        lookups = [LookupConfig("customerID", dim, "customer_nk", "customer_sk", "customer_sk")]
        pipeline.resolve_batch(fact, lookups)
        assert "customer_sk" not in fact.columns
