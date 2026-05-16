"""Tests for DimBuilder (Kimball Subsystem #13 — dim portion)."""

import pandas as pd
import pytest

from src.deliver.dim_builder import DimBuilder
from src.deliver.surrogate_key_generator import SurrogateKeyGenerator


@pytest.fixture
def sk_gen(tmp_path):
    return SurrogateKeyGenerator(meta_dir=tmp_path)


@pytest.fixture
def builder(sk_gen, tmp_path):
    return DimBuilder(sk_gen=sk_gen, config_dir=tmp_path / "config")


class TestBuildDimDate:
    def test_required_columns_present(self, builder):
        df = builder.build_dim_date(start_year=2020, end_year=2020)
        required = {
            "date_sk", "full_date", "day_of_week", "day_name",
            "day_of_month", "day_of_year", "week_of_year",
            "month", "month_name", "quarter", "year", "is_weekend",
            "fiscal_year", "fiscal_quarter",
        }
        assert required.issubset(set(df.columns))

    def test_correct_row_count_for_year(self, builder):
        df = builder.build_dim_date(start_year=2020, end_year=2020)
        assert len(df) == 366  # 2020 is a leap year

    def test_date_sk_format(self, builder):
        df = builder.build_dim_date(start_year=2020, end_year=2020)
        assert df["date_sk"].iloc[0] == 20200101
        assert df["date_sk"].iloc[-1] == 20201231

    def test_is_weekend_flag(self, builder):
        df = builder.build_dim_date(start_year=2024, end_year=2024)
        saturdays = df[df["day_name"] == "Saturday"]
        assert saturdays["is_weekend"].all()
        mondays = df[df["day_name"] == "Monday"]
        assert not mondays["is_weekend"].any()

    def test_quarter_values(self, builder):
        df = builder.build_dim_date(start_year=2024, end_year=2024)
        assert df[df["month"] == 1]["quarter"].iloc[0] == 1
        assert df[df["month"] == 4]["quarter"].iloc[0] == 2
        assert df[df["month"] == 7]["quarter"].iloc[0] == 3
        assert df[df["month"] == 10]["quarter"].iloc[0] == 4


class TestBuildDimCustomer:
    def _conformed_df(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "customerID": "ALFKI",
            "companyName": "Alfreds Futterkiste",
            "contactName": "Maria Anders",
            "contactTitle": "Sales Representative",
            "address": "Obere Str. 57",
            "city": "Berlin",
            "postalCode": "12209",
            "phone": "030-0074321",
            "country_code": "DE",
        }])

    def test_sk_column_inserted(self, builder):
        df = builder.build_dim_customer(self._conformed_df(), pd.DataFrame())
        assert "customer_sk" in df.columns
        assert df["customer_sk"].iloc[0] == 1

    def test_camelcase_renamed_to_snake(self, builder):
        df = builder.build_dim_customer(self._conformed_df(), pd.DataFrame())
        assert "customer_nk" in df.columns
        assert "company_name" in df.columns
        assert "contact_name" in df.columns

    def test_region_name_from_geography(self, builder):
        geo = pd.DataFrame([{"country_code": "DE", "region": "Europe"}])
        df = builder.build_dim_customer(self._conformed_df(), geo)
        assert df["region_name"].iloc[0] == "Europe"

    def test_region_name_none_when_no_geo(self, builder):
        df = builder.build_dim_customer(self._conformed_df(), pd.DataFrame())
        assert df["region_name"].iloc[0] is None

    def test_scd_defaults_set(self, builder):
        df = builder.build_dim_customer(self._conformed_df(), pd.DataFrame())
        assert df["is_current"].iloc[0] == True
        assert df["expiration_date"].iloc[0] is None
        assert df["audit_sk"].iloc[0] == -1


class TestBuildDimProduct:
    def _products(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "productID": 1, "productName": "Chai", "categoryID": 1,
            "supplierID": 1, "unitPrice": 18.0, "unitsInStock": 39,
            "quantityPerUnit": "10 boxes x 20 bags", "discontinued": 0,
        }])

    def _categories(self) -> pd.DataFrame:
        return pd.DataFrame([{"categoryID": 1, "categoryName": "Beverages"}])

    def _suppliers(self) -> pd.DataFrame:
        return pd.DataFrame([{"supplierID": 1, "companyName": "Exotic Liquids", "country": "UK"}])

    def test_category_name_joined(self, builder):
        df = builder.build_dim_product(self._products(), self._categories(), self._suppliers())
        assert df["category_name"].iloc[0] == "Beverages"

    def test_supplier_name_joined(self, builder):
        df = builder.build_dim_product(self._products(), self._categories(), self._suppliers())
        assert df["supplier_name"].iloc[0] == "Exotic Liquids"

    def test_product_sk_assigned(self, builder):
        df = builder.build_dim_product(self._products(), self._categories(), self._suppliers())
        assert "product_sk" in df.columns
        assert df["product_sk"].iloc[0] == 1

    def test_empty_categories_graceful(self, builder):
        df = builder.build_dim_product(self._products(), pd.DataFrame(), pd.DataFrame())
        assert "product_sk" in df.columns
        assert len(df) == 1


class TestBuildDimEmployee:
    def _employees(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "employeeID": 1,
            "titleOfCourtesy": "Mr.",
            "firstName": "Steven",
            "lastName": "Buchanan",
            "title": "Sales Manager",
            "reportsTo": 2,
            "hireDate": "1993-10-17",
            "city": "London",
            "country": "UK",
        }])

    def test_full_name_concatenated(self, builder):
        df = builder.build_dim_employee(self._employees())
        assert df["full_name"].iloc[0] == "Mr. Steven Buchanan"

    def test_employee_sk_assigned(self, builder):
        df = builder.build_dim_employee(self._employees())
        assert df["employee_sk"].iloc[0] == 1

    def test_reports_to_nk_numeric(self, builder):
        df = builder.build_dim_employee(self._employees())
        assert df["reports_to_nk"].iloc[0] == 2.0


class TestBuildDimShipper:
    def _shippers(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"shipperID": 1, "companyName": "Speedy Express", "phone": "(503) 555-9831"},
            {"shipperID": 2, "companyName": "United Package", "phone": "(503) 555-3199"},
        ])

    def test_shipper_sk_assigned(self, builder):
        df = builder.build_dim_shipper(self._shippers())
        assert list(df["shipper_sk"]) == [1, 2]

    def test_columns_present(self, builder):
        df = builder.build_dim_shipper(self._shippers())
        assert set(df.columns) == {"shipper_sk", "shipper_nk", "company_name", "phone", "audit_sk"}

    def test_shipper_nk_numeric(self, builder):
        df = builder.build_dim_shipper(self._shippers())
        assert df["shipper_nk"].dtype in (float, int) or str(df["shipper_nk"].dtype).startswith("int")
