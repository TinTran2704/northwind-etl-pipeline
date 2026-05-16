"""Tests for AggregateBuilder (Kimball Subsystem #19)."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.deliver.aggregate_builder import AggregateBuilder


@pytest.fixture
def builder():
    return AggregateBuilder()


def _make_fact(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _make_dim_date(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _make_dim_customer(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestBuildAggSalesMonthly:
    def _standard_inputs(self):
        fact = _make_fact([
            {"order_id": 1, "order_date_sk": 20240115, "product_sk": 10, "customer_sk": 100,
             "quantity": 5, "net_amount": 100.0},
            {"order_id": 2, "order_date_sk": 20240220, "product_sk": 10, "customer_sk": 100,
             "quantity": 3, "net_amount": 60.0},
            {"order_id": 3, "order_date_sk": 20240310, "product_sk": 20, "customer_sk": 200,
             "quantity": 7, "net_amount": 140.0},
        ])
        dim_date = _make_dim_date([
            {"date_sk": 20240115, "year": 2024, "month": 1},
            {"date_sk": 20240220, "year": 2024, "month": 2},
            {"date_sk": 20240310, "year": 2024, "month": 3},
        ])
        dim_customer = _make_dim_customer([
            {"customer_sk": 100, "country_code": "DE", "is_current": True},
            {"customer_sk": 200, "country_code": "FR", "is_current": True},
        ])
        return fact, dim_date, dim_customer

    def test_returns_rows_per_grain(self, builder):
        fact, dim_date, dim_cust = self._standard_inputs()
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        # 3 rows: Jan/prod10/DE, Feb/prod10/DE, Mar/prod20/FR
        assert len(agg) == 3

    def test_year_month_format(self, builder):
        fact, dim_date, dim_cust = self._standard_inputs()
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        jan_row = agg[agg["year_month"] == 202401]
        assert len(jan_row) == 1

    def test_total_net_amount_summed(self, builder):
        fact, dim_date, dim_cust = self._standard_inputs()
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        jan = agg[agg["year_month"] == 202401]
        assert abs(jan["total_net_amount"].iloc[0] - 100.0) < 0.01

    def test_total_quantity_summed(self, builder):
        fact, dim_date, dim_cust = self._standard_inputs()
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        jan = agg[agg["year_month"] == 202401]
        assert jan["total_quantity"].iloc[0] == 5

    def test_order_count_distinct_orders(self, builder):
        # Two rows with same order_id in same month — should count as 1
        fact = _make_fact([
            {"order_id": 1, "order_date_sk": 20240115, "product_sk": 10, "customer_sk": 100,
             "quantity": 5, "net_amount": 100.0},
            {"order_id": 1, "order_date_sk": 20240115, "product_sk": 11, "customer_sk": 100,
             "quantity": 3, "net_amount": 60.0},
        ])
        dim_date = _make_dim_date([{"date_sk": 20240115, "year": 2024, "month": 1}])
        dim_cust = _make_dim_customer([{"customer_sk": 100, "country_code": "DE", "is_current": True}])
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        # product_sk differs → 2 rows, each with order_count=1
        for _, row in agg.iterrows():
            assert row["order_count"] == 1

    def test_empty_fact_returns_empty(self, builder):
        agg = builder.build_agg_sales_monthly(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        assert agg.empty

    def test_audit_sk_stamped(self, builder):
        fact, dim_date, dim_cust = self._standard_inputs()
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust, audit_sk=99)
        assert (agg["audit_sk"] == 99).all()

    def test_customer_country_fallback_to_zz(self, builder):
        fact = _make_fact([
            {"order_id": 1, "order_date_sk": 20240115, "product_sk": 10, "customer_sk": 999,
             "quantity": 5, "net_amount": 100.0},
        ])
        dim_date = _make_dim_date([{"date_sk": 20240115, "year": 2024, "month": 1}])
        dim_cust = _make_dim_customer([{"customer_sk": 100, "country_code": "DE", "is_current": True}])
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        assert agg.iloc[0]["customer_country"] == "ZZ"

    def test_missing_dim_date_columns_returns_empty(self, builder):
        fact = _make_fact([{"order_id": 1, "order_date_sk": 20240115, "product_sk": 10,
                            "customer_sk": 100, "quantity": 5, "net_amount": 100.0}])
        dim_date_bad = pd.DataFrame([{"date_sk": 20240115}])  # no year/month columns
        agg = builder.build_agg_sales_monthly(fact, dim_date_bad, pd.DataFrame(), pd.DataFrame())
        assert agg.empty

    def test_required_output_columns(self, builder):
        fact, dim_date, dim_cust = self._standard_inputs()
        agg = builder.build_agg_sales_monthly(fact, dim_date, pd.DataFrame(), dim_cust)
        required = {"year_month", "product_sk", "customer_country",
                    "total_quantity", "total_net_amount", "order_count", "audit_sk"}
        assert required.issubset(set(agg.columns))


class TestLoadAggToPostgres:
    def test_empty_df_returns_zero(self, builder):
        engine = MagicMock()
        assert builder.load_agg_to_postgres(pd.DataFrame(), engine) == 0

    def test_returns_row_count(self, builder):
        agg = pd.DataFrame([{
            "year_month": 202401, "product_sk": 1, "customer_country": "DE",
            "total_quantity": 5, "total_net_amount": 100.0, "order_count": 1, "audit_sk": -1,
        }])
        engine = MagicMock()
        conn = engine.begin.return_value.__enter__.return_value
        assert builder.load_agg_to_postgres(agg, engine) == 1
