"""
Fact Builder — Kimball Subsystem #13 (fact portion).

Builds fact_sales from orders + order_details, resolves all surrogate keys
via SurrogateKeyPipeline, computes derived measures, and loads to PostgreSQL.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text

from src.deliver.surrogate_key_pipeline import LookupConfig, SurrogateKeyPipeline

logger = logging.getLogger(__name__)

_UNKNOWN_DATE_SK = 19000101
_UNKNOWN_SK = -1


class FactBuilderError(Exception):
    """Raised when fact build or load fails."""


class FactBuilder:
    """Build and load fact_sales.

    Args:
        sk_pipeline: SurrogateKeyPipeline for FK resolution.
    """

    def __init__(self, sk_pipeline: Optional[SurrogateKeyPipeline] = None) -> None:
        self._sk = sk_pipeline or SurrogateKeyPipeline()

    def build_fact_sales(
        self,
        orders_df: pd.DataFrame,
        order_details_df: pd.DataFrame,
        all_dims: dict[str, pd.DataFrame],
        batch_id: str,
        audit_sk: int = -1,
    ) -> pd.DataFrame:
        """Merge orders + order_details, resolve all SKs, compute measures.

        Args:
            orders_df:       Raw orders DataFrame (from orders.csv).
            order_details_df:Raw order_details DataFrame (from order-details.csv).
            all_dims:        Dict {dim_name: DataFrame} for SK resolution.
            batch_id:        ETL batch id (informational).
            audit_sk:        audit_sk to stamp on every fact row.

        Returns:
            DataFrame matching warehouse.fact_sales schema.
        """
        if orders_df.empty or order_details_df.empty:
            logger.warning("[DELIVER] fact_sales: empty orders or order_details — returning empty")
            return pd.DataFrame()

        orders = orders_df.copy()
        details = order_details_df.copy()

        # Normalise column names (both CSVs use camelCase)
        orders.columns = orders.columns.str.strip()
        details.columns = details.columns.str.strip()

        # Merge on orderID
        merged = details.merge(orders, on="orderID", how="inner", suffixes=("_det", "_ord"))

        # line_number = row rank within each order
        merged["line_number"] = (
            merged.groupby("orderID").cumcount() + 1
        ).astype("int16")

        # Date SKs
        merged["order_date_sk"]    = merged["orderDate"].apply(self._sk.date_to_sk)
        merged["required_date_sk"] = merged["requiredDate"].apply(self._sk.date_to_sk)
        merged["shipped_date_sk"]  = merged.get("shippedDate", pd.Series(dtype=str)).apply(
            lambda v: self._sk.date_to_sk(v) if pd.notna(v) and str(v).lower() != "null" else _UNKNOWN_DATE_SK
        )

        # Parse orderDate for point-in-time SK lookups
        merged["_order_date"] = pd.to_datetime(merged["orderDate"], errors="coerce")

        # Resolve dimension SKs
        lookups: list[LookupConfig] = []

        dim_customer = all_dims.get("dim_customer", pd.DataFrame())
        if not dim_customer.empty and "customer_nk" in dim_customer.columns:
            lookups.append(LookupConfig(
                source_col="customerID", dim_df=dim_customer,
                nk_col="customer_nk", sk_col="customer_sk",
                output_col="customer_sk", date_col="_order_date",
            ))
        else:
            merged["customer_sk"] = _UNKNOWN_SK

        dim_employee = all_dims.get("dim_employee", pd.DataFrame())
        if not dim_employee.empty and "employee_nk" in dim_employee.columns:
            lookups.append(LookupConfig(
                source_col="employeeID", dim_df=dim_employee,
                nk_col="employee_nk", sk_col="employee_sk",
                output_col="employee_sk", date_col="_order_date",
            ))
        else:
            merged["employee_sk"] = _UNKNOWN_SK

        dim_product = all_dims.get("dim_product", pd.DataFrame())
        if not dim_product.empty and "product_nk" in dim_product.columns:
            # Convert productID to int for matching
            merged["productID"] = pd.to_numeric(merged["productID"], errors="coerce")
            lookups.append(LookupConfig(
                source_col="productID", dim_df=dim_product,
                nk_col="product_nk", sk_col="product_sk",
                output_col="product_sk", date_col="_order_date",
            ))
        else:
            merged["product_sk"] = _UNKNOWN_SK

        dim_shipper = all_dims.get("dim_shipper", pd.DataFrame())
        if not dim_shipper.empty and "shipper_nk" in dim_shipper.columns:
            merged["shipVia"] = pd.to_numeric(merged.get("shipVia", pd.Series(dtype=str)), errors="coerce")
            lookups.append(LookupConfig(
                source_col="shipVia", dim_df=dim_shipper,
                nk_col="shipper_nk", sk_col="shipper_sk",
                output_col="shipper_sk",  # Type 1 — no date_col
            ))
        else:
            merged["shipper_sk"] = _UNKNOWN_SK

        dim_geography = all_dims.get("dim_geography", pd.DataFrame())
        if not dim_geography.empty and "country_code" in dim_geography.columns:
            # Standardise shipCountry → country_code, then lookup geography_sk
            from src.conform.standardizer import Standardizer
            std = Standardizer()
            merged["_ship_cc"] = merged.get("shipCountry", pd.Series(dtype=str)).apply(
                lambda v: std.standardize_country(v) if pd.notna(v) else None
            )
            lookups.append(LookupConfig(
                source_col="_ship_cc", dim_df=dim_geography,
                nk_col="country_code", sk_col="geography_sk",
                output_col="ship_geography_sk",
            ))
        else:
            merged["ship_geography_sk"] = _UNKNOWN_SK

        if lookups:
            merged = self._sk.resolve_batch(merged, lookups)

        # Fill missing SK columns
        for col in ("customer_sk", "employee_sk", "product_sk", "shipper_sk", "ship_geography_sk"):
            if col not in merged.columns:
                merged[col] = _UNKNOWN_SK

        # unit_price: use order_details value (more accurate at transaction time)
        unit_price_col = "unitPrice_det" if "unitPrice_det" in merged.columns else "unitPrice"
        merged["unit_price"] = pd.to_numeric(merged[unit_price_col], errors="coerce").fillna(0.0)
        merged["quantity"]   = pd.to_numeric(merged["quantity"], errors="coerce").fillna(0).astype(int)
        merged["discount"]   = pd.to_numeric(merged["discount"], errors="coerce").fillna(0.0).clip(0, 1)

        # Derived measures
        merged["extended_price"]  = (merged["quantity"] * merged["unit_price"]).round(2)
        merged["discount_amount"] = (merged["extended_price"] * merged["discount"]).round(2)
        merged["net_amount"]      = (merged["extended_price"] - merged["discount_amount"]).round(2)

        # Freight allocation: order.freight × (line_extended / order_total_extended)
        merged["freight"] = pd.to_numeric(merged.get("freight", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        order_totals = merged.groupby("orderID")["extended_price"].transform("sum")
        merged["freight_allocated"] = (
            merged["freight"] * merged["extended_price"] / order_totals.replace(0, 1)
        ).round(2)

        merged["audit_sk"] = audit_sk

        # Select final columns matching warehouse schema
        fact = merged[[
            "orderID", "line_number",
            "order_date_sk", "required_date_sk", "shipped_date_sk",
            "customer_sk", "employee_sk", "product_sk", "shipper_sk",
            "ship_geography_sk", "audit_sk",
            "quantity", "unit_price", "discount",
            "extended_price", "discount_amount", "net_amount", "freight_allocated",
        ]].copy()

        fact = fact.rename(columns={"orderID": "order_id"})
        fact["order_id"] = fact["order_id"].astype(int)

        # Drop rows with non-positive quantity (data quality guard)
        fact = fact[fact["quantity"] > 0]

        logger.info("[DELIVER] batch=%s fact_sales built: %d rows", batch_id, len(fact))
        return fact

    def load_fact_to_postgres(self, df: pd.DataFrame, engine: Engine) -> int:
        """Bulk-insert fact_sales rows using executemany.

        Uses ON CONFLICT (order_id, line_number) DO NOTHING for idempotency.

        Args:
            df:     fact_sales DataFrame.
            engine: SQLAlchemy engine.

        Returns:
            Number of rows inserted.
        """
        if df.empty:
            return 0

        col_list     = ", ".join(df.columns)
        placeholders = ", ".join(f":{c}" for c in df.columns)
        sql = text(f"""
            INSERT INTO warehouse.fact_sales ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (order_id, line_number) DO NOTHING
        """)

        rows = df.where(pd.notna(df), None).to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info("[DELIVER] fact_sales: inserted %d rows", len(df))
        return len(df)
