"""Tests for FactBuilder (Kimball Subsystem #13 — fact portion)."""

import pandas as pd
import pytest

from src.deliver.fact_builder import FactBuilder


@pytest.fixture
def builder():
    return FactBuilder()


def _orders() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "orderID": 10248,
            "customerID": "VINET",
            "employeeID": 5,
            "orderDate": "1996-07-04",
            "requiredDate": "1996-08-01",
            "shippedDate": "1996-07-16",
            "shipVia": 3,
            "freight": 32.38,
            "shipCountry": "France",
        },
        {
            "orderID": 10249,
            "customerID": "TOMSP",
            "employeeID": 6,
            "orderDate": "1996-07-05",
            "requiredDate": "1996-08-16",
            "shippedDate": "1996-07-10",
            "shipVia": 1,
            "freight": 11.61,
            "shipCountry": "Germany",
        },
    ])


def _details() -> pd.DataFrame:
    return pd.DataFrame([
        # Order 10248 — 2 lines
        {"orderID": 10248, "productID": 11, "unitPrice": 14.0, "quantity": 12, "discount": 0.0},
        {"orderID": 10248, "productID": 42, "unitPrice": 9.8,  "quantity": 10, "discount": 0.0},
        # Order 10249 — 1 line
        {"orderID": 10249, "productID": 14, "unitPrice": 18.6, "quantity": 9,  "discount": 0.0},
    ])


class TestBuildFactSalesColumns:
    def test_all_expected_columns_present(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        expected = {
            "order_id", "line_number",
            "order_date_sk", "required_date_sk", "shipped_date_sk",
            "customer_sk", "employee_sk", "product_sk", "shipper_sk",
            "ship_geography_sk", "audit_sk",
            "quantity", "unit_price", "discount",
            "extended_price", "discount_amount", "net_amount", "freight_allocated",
        }
        assert expected.issubset(set(fact.columns))

    def test_returns_three_rows(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        assert len(fact) == 3

    def test_order_id_renamed(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        assert "order_id" in fact.columns
        assert "orderID" not in fact.columns


class TestLineNumber:
    def test_line_numbers_start_at_one_per_order(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        order_10248 = fact[fact["order_id"] == 10248].sort_values("line_number")
        assert list(order_10248["line_number"]) == [1, 2]

    def test_single_line_order_has_line_number_one(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        order_10249 = fact[fact["order_id"] == 10249]
        assert order_10249["line_number"].iloc[0] == 1


class TestMeasures:
    def test_extended_price_computed(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        row = fact[(fact["order_id"] == 10248) & (fact["line_number"] == 1)].iloc[0]
        assert row["extended_price"] == round(12 * 14.0, 2)

    def test_discount_amount_computed(self, builder):
        details = pd.DataFrame([
            {"orderID": 10248, "productID": 11, "unitPrice": 14.0, "quantity": 12, "discount": 0.1},
        ])
        orders = _orders().head(1)
        fact = builder.build_fact_sales(orders, details, {}, "batch-001")
        row = fact.iloc[0]
        expected_ext = round(12 * 14.0, 2)
        expected_disc = round(expected_ext * 0.1, 2)
        assert row["discount_amount"] == expected_disc

    def test_net_amount_equals_extended_minus_discount(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        for _, row in fact.iterrows():
            assert abs(row["net_amount"] - (row["extended_price"] - row["discount_amount"])) < 0.005

    def test_discount_clipped_to_one(self, builder):
        details = pd.DataFrame([
            {"orderID": 10248, "productID": 11, "unitPrice": 14.0, "quantity": 12, "discount": 1.5},
        ])
        fact = builder.build_fact_sales(_orders().head(1), details, {}, "batch-001")
        assert fact.iloc[0]["discount"] <= 1.0


class TestFreightAllocation:
    def test_freight_sum_equals_order_freight(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        order_freight = {10248: 32.38, 10249: 11.61}
        for order_id, expected_freight in order_freight.items():
            allocated_sum = fact[fact["order_id"] == order_id]["freight_allocated"].sum()
            assert abs(allocated_sum - expected_freight) < 0.02

    def test_single_line_gets_full_freight(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        row_10249 = fact[fact["order_id"] == 10249].iloc[0]
        assert abs(row_10249["freight_allocated"] - 11.61) < 0.01


class TestEdgeCases:
    def test_empty_orders_returns_empty(self, builder):
        fact = builder.build_fact_sales(pd.DataFrame(), _details(), {}, "batch-001")
        assert fact.empty

    def test_empty_details_returns_empty(self, builder):
        fact = builder.build_fact_sales(_orders(), pd.DataFrame(), {}, "batch-001")
        assert fact.empty

    def test_nonpositive_quantity_filtered(self, builder):
        details = pd.DataFrame([
            {"orderID": 10248, "productID": 11, "unitPrice": 14.0, "quantity": 0,  "discount": 0.0},
            {"orderID": 10248, "productID": 42, "unitPrice": 9.8,  "quantity": 10, "discount": 0.0},
        ])
        fact = builder.build_fact_sales(_orders().head(1), details, {}, "batch-001")
        assert len(fact) == 1
        assert fact.iloc[0]["quantity"] == 10

    def test_null_shipped_date_gets_unknown_sk(self, builder):
        orders = _orders().head(1).copy()
        orders["shippedDate"] = None
        details = _details()[_details()["orderID"] == 10248]
        fact = builder.build_fact_sales(orders, details, {}, "batch-001")
        assert fact["shipped_date_sk"].iloc[0] == 19000101


class TestDateSks:
    def test_order_date_sk_format(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001")
        row = fact[fact["order_id"] == 10248].iloc[0]
        assert row["order_date_sk"] == 19960704

    def test_audit_sk_stamped(self, builder):
        fact = builder.build_fact_sales(_orders(), _details(), {}, "batch-001", audit_sk=42)
        assert (fact["audit_sk"] == 42).all()
