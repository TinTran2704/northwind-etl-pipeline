"""
Aggregate Builder — Kimball Subsystem #19.

Pre-computes agg_sales_monthly from fact_sales + dimension lookups.
Grain: year_month × product_sk × customer_country.
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)


class AggregateBuilderError(Exception):
    """Raised when aggregate build or load fails."""


class AggregateBuilder:
    """Build and load agg_sales_monthly."""

    def build_agg_sales_monthly(
        self,
        fact_df: pd.DataFrame,
        dim_date: pd.DataFrame,
        dim_product: pd.DataFrame,
        dim_customer: pd.DataFrame,
        audit_sk: int = -1,
    ) -> pd.DataFrame:
        """Compute monthly sales aggregates.

        Joins fact_sales to dim_date (for year/month), dim_product (for
        product_sk), and dim_customer (for country_code).  Groups by
        year_month × product_sk × customer_country.

        Args:
            fact_df:      fact_sales DataFrame (warehouse schema).
            dim_date:     dim_date DataFrame.
            dim_product:  dim_product DataFrame (for category_name if needed).
            dim_customer: dim_customer DataFrame (for country_code).
            audit_sk:     audit_sk to stamp on each aggregate row.

        Returns:
            DataFrame matching warehouse.agg_sales_monthly schema.
        """
        if fact_df.empty:
            logger.info("[DELIVER] agg_sales_monthly: empty fact_sales — skipping")
            return pd.DataFrame()

        # Join dim_date for year + month
        date_cols = [c for c in ("date_sk", "year", "month") if c in dim_date.columns]
        if "date_sk" not in dim_date.columns or "year" not in dim_date.columns:
            logger.warning("[DELIVER] dim_date missing required columns — agg skipped")
            return pd.DataFrame()

        fact = fact_df.copy()
        date_sub = dim_date[["date_sk", "year", "month"]].drop_duplicates("date_sk")
        fact = fact.merge(date_sub, left_on="order_date_sk", right_on="date_sk", how="left")

        # Join dim_customer for country_code
        if not dim_customer.empty and "customer_sk" in dim_customer.columns and "country_code" in dim_customer.columns:
            cust_sub = dim_customer[dim_customer["is_current"] == True][["customer_sk", "country_code"]].drop_duplicates("customer_sk")
            fact = fact.merge(cust_sub, on="customer_sk", how="left")
            fact["customer_country"] = fact["country_code"].fillna("ZZ")
        else:
            fact["customer_country"] = "ZZ"

        # Compute year_month
        fact["year"]  = pd.to_numeric(fact.get("year", pd.Series(dtype=float)), errors="coerce").fillna(0).astype(int)
        fact["month"] = pd.to_numeric(fact.get("month", pd.Series(dtype=float)), errors="coerce").fillna(0).astype(int)
        fact["year_month"] = fact["year"] * 100 + fact["month"]

        agg = (
            fact
            .groupby(["year_month", "product_sk", "customer_country"], as_index=False)
            .agg(
                total_quantity  =("quantity",   "sum"),
                total_net_amount=("net_amount", "sum"),
                order_count     =("order_id",   "nunique"),
            )
        )

        agg["total_net_amount"] = agg["total_net_amount"].round(2)
        agg["audit_sk"] = audit_sk

        logger.info("[DELIVER] agg_sales_monthly: %d rows", len(agg))
        return agg

    def load_agg_to_postgres(self, df: pd.DataFrame, engine: Engine) -> int:
        """Upsert agg_sales_monthly rows.

        Uses ON CONFLICT (year_month, product_sk, customer_country) DO UPDATE
        so repeated runs refresh totals rather than duplicating.

        Args:
            df:     agg_sales_monthly DataFrame.
            engine: SQLAlchemy engine.

        Returns:
            Number of rows upserted.
        """
        if df.empty:
            return 0

        sql = text("""
            INSERT INTO warehouse.agg_sales_monthly
                (year_month, product_sk, customer_country,
                 total_quantity, total_net_amount, order_count, audit_sk)
            VALUES
                (:year_month, :product_sk, :customer_country,
                 :total_quantity, :total_net_amount, :order_count, :audit_sk)
            ON CONFLICT (year_month, product_sk, customer_country)
            DO UPDATE SET
                total_quantity   = EXCLUDED.total_quantity,
                total_net_amount = EXCLUDED.total_net_amount,
                order_count      = EXCLUDED.order_count,
                audit_sk         = EXCLUDED.audit_sk
        """)

        rows = df.where(pd.notna(df), None).to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info("[DELIVER] agg_sales_monthly: upserted %d rows", len(df))
        return len(df)
