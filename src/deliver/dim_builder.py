"""
Dimension Builder — Kimball Subsystem #13 (dim portion).

Builds and loads dimension tables to PostgreSQL warehouse schema.
Handles dim_date (static), and SCD Type 1/2 dimensions.
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text

from src.conform.standardizer import Standardizer
from src.deliver.surrogate_key_generator import SurrogateKeyGenerator

logger = logging.getLogger(__name__)

# SCD Type 2 columns per dim — changes trigger a new row.
_TYPE2_COLS: dict[str, list[str]] = {
    "dim_customer": ["company_name", "address", "city", "postal_code", "country_code"],
    "dim_product":  ["product_name", "unit_price", "discontinued", "category_name"],
    "dim_employee": ["title", "city", "country_code"],
}

# SCD Type 1 columns per dim — changes update all rows in-place.
_TYPE1_COLS: dict[str, list[str]] = {
    "dim_customer": ["contact_name", "contact_title", "phone"],
    "dim_product":  ["units_in_stock", "quantity_per_unit"],
    "dim_employee": ["reports_to_nk"],
}


class DimBuilderError(Exception):
    """Raised when dimension build or load fails."""


class DimBuilder:
    """Build and load dimension DataFrames.

    Args:
        sk_gen:     SurrogateKeyGenerator for SK allocation.
        config_dir: Config directory for Standardizer.
    """

    def __init__(
        self,
        sk_gen: Optional[SurrogateKeyGenerator] = None,
        config_dir: Path = Path("config"),
    ) -> None:
        self._sk_gen = sk_gen or SurrogateKeyGenerator()
        self._std = Standardizer(config_dir=config_dir)

    # ------------------------------------------------------------------
    # Build methods (return DataFrames, no DB)
    # ------------------------------------------------------------------

    def build_dim_date(self, start_year: int = 1990, end_year: int = 2030) -> pd.DataFrame:
        """Generate a complete dim_date for the given year range.

        date_sk format: YYYYMMDD (integer).

        Args:
            start_year: First year (inclusive).
            end_year:   Last year (inclusive).

        Returns:
            DataFrame matching warehouse.dim_date schema.
        """
        dates = pd.date_range(
            start=f"{start_year}-01-01",
            end=f"{end_year}-12-31",
            freq="D",
        )
        day_names  = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        month_names = ["","January","February","March","April","May","June",
                       "July","August","September","October","November","December"]

        rows = []
        for d in dates:
            q = (d.month - 1) // 3 + 1
            rows.append({
                "date_sk":       int(d.strftime("%Y%m%d")),
                "full_date":     d.date(),
                "day_of_week":   d.dayofweek,          # 0=Mon
                "day_name":      day_names[d.dayofweek],
                "day_of_month":  d.day,
                "day_of_year":   d.dayofyear,
                "week_of_year":  int(d.strftime("%V")),
                "month":         d.month,
                "month_name":    month_names[d.month],
                "quarter":       q,
                "year":          d.year,
                "is_weekend":    d.dayofweek >= 5,
                "fiscal_year":   d.year,
                "fiscal_quarter": q,
            })
        return pd.DataFrame(rows)

    def build_dim_customer(self, conformed_df: pd.DataFrame, dim_geography: pd.DataFrame) -> pd.DataFrame:
        """Map conformed dim_customer columns to warehouse schema.

        Args:
            conformed_df:  DataFrame from DimensionManager.get_latest("dim_customer").
            dim_geography: For region_name lookup.

        Returns:
            DataFrame matching warehouse.dim_customer schema.
        """
        df = conformed_df.copy()

        # Column mapping: conformed → warehouse
        col_map = {
            "customerID":   "customer_nk",
            "companyName":  "company_name",
            "contactName":  "contact_name",
            "contactTitle": "contact_title",
            "address":      "address",
            "city":         "city",
            "postalCode":   "postal_code",
            "phone":        "phone",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # Ensure country_code exists
        if "country_code" not in df.columns and "country" in df.columns:
            df["country_code"] = df["country"].apply(self._std.standardize_country)

        # region_name from dim_geography lookup
        if not dim_geography.empty and "country_code" in dim_geography.columns:
            geo_map = dim_geography.set_index("country_code")["region"].to_dict()
            df["region_name"] = df.get("country_code", pd.Series(dtype=str)).map(geo_map)
        else:
            df["region_name"] = None

        # SCD columns (set by conform pipeline; default if missing)
        today = str(date.today())
        if "effective_date" not in df.columns:
            df["effective_date"] = today
        if "expiration_date" not in df.columns:
            df["expiration_date"] = None
        if "is_current" not in df.columns:
            df["is_current"] = True

        df["audit_sk"] = -1

        # Assign SKs
        nk_col = "customer_nk"
        required = [nk_col, "company_name", "effective_date", "is_current"]
        df = df[[c for c in df.columns if c in required or c in col_map.values() or c in
                 ("country_code", "region_name", "expiration_date", "audit_sk")]]

        sks = self._sk_gen.batch_next_sk("dim_customer", len(df))
        df.insert(0, "customer_sk", sks)
        return df

    def build_dim_product(
        self,
        products_df: pd.DataFrame,
        categories_df: pd.DataFrame,
        suppliers_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build dim_product by joining products, categories, suppliers.

        Args:
            products_df:   From products.csv.
            categories_df: From categories.csv.
            suppliers_df:  From suppliers.csv.

        Returns:
            DataFrame matching warehouse.dim_product schema.
        """
        df = products_df.copy()

        # Join categories
        if not categories_df.empty:
            cat = categories_df[["categoryID", "categoryName"]].copy()
            df = df.merge(cat, on="categoryID", how="left")
        else:
            df["categoryName"] = None

        # Join suppliers
        if not suppliers_df.empty:
            sup = suppliers_df[["supplierID", "companyName", "country"]].copy()
            sup = sup.rename(columns={"companyName": "supplierName", "country": "supplierCountry"})
            df = df.merge(sup, on="supplierID", how="left")
            df["supplier_country"] = df["supplierCountry"].apply(self._std.standardize_country)
        else:
            df["supplierName"] = None
            df["supplier_country"] = None

        today = str(date.today())
        out = pd.DataFrame({
            "product_nk":       pd.to_numeric(df["productID"], errors="coerce"),
            "product_name":     df.get("productName", pd.Series(dtype=str)),
            "category_name":    df.get("categoryName", pd.Series(dtype=str)),
            "supplier_name":    df.get("supplierName", pd.Series(dtype=str)),
            "supplier_country": df.get("supplier_country", pd.Series(dtype=str)),
            "quantity_per_unit":df.get("quantityPerUnit", pd.Series(dtype=str)),
            "unit_price":       pd.to_numeric(df.get("unitPrice", pd.Series(dtype=float)), errors="coerce"),
            "units_in_stock":   pd.to_numeric(df.get("unitsInStock", pd.Series(dtype=float)), errors="coerce"),
            "discontinued":     df.get("discontinued", pd.Series(dtype=object)).apply(
                                    lambda v: bool(int(v)) if pd.notna(v) else False),
            "effective_date":   today,
            "expiration_date":  None,
            "is_current":       True,
            "audit_sk":         -1,
        })

        sks = self._sk_gen.batch_next_sk("dim_product", len(out))
        out.insert(0, "product_sk", sks)
        return out

    def build_dim_employee(self, employees_df: pd.DataFrame) -> pd.DataFrame:
        """Build dim_employee.

        Args:
            employees_df: From employees.csv.

        Returns:
            DataFrame matching warehouse.dim_employee schema.
        """
        df = employees_df.copy()
        today = str(date.today())

        def _full_name(row) -> str:
            parts = [str(row.get("titleOfCourtesy") or "").strip(),
                     str(row.get("firstName") or "").strip(),
                     str(row.get("lastName") or "").strip()]
            return " ".join(p for p in parts if p and p.lower() != "null")

        out = pd.DataFrame({
            "employee_nk":    pd.to_numeric(df["employeeID"], errors="coerce"),
            "full_name":      df.apply(_full_name, axis=1),
            "title":          df.get("title", pd.Series(dtype=str)).where(df.get("title", pd.Series(dtype=str)) != "NULL", None),
            "reports_to_nk":  pd.to_numeric(df.get("reportsTo", pd.Series(dtype=str)), errors="coerce"),
            "hire_date":      pd.to_datetime(df.get("hireDate", pd.Series(dtype=str)), errors="coerce").dt.date,
            "city":           df.get("city", pd.Series(dtype=str)).apply(self._std.title_case),
            "country_code":   df.get("country", pd.Series(dtype=str)).apply(self._std.standardize_country),
            "effective_date": today,
            "expiration_date":None,
            "is_current":     True,
            "audit_sk":       -1,
        })

        sks = self._sk_gen.batch_next_sk("dim_employee", len(out))
        out.insert(0, "employee_sk", sks)
        return out

    def build_dim_shipper(self, shippers_df: pd.DataFrame) -> pd.DataFrame:
        """Build dim_shipper (Type 1 — no SCD2 history).

        Args:
            shippers_df: From shippers.csv.

        Returns:
            DataFrame matching warehouse.dim_shipper schema.
        """
        df = shippers_df.copy()
        out = pd.DataFrame({
            "shipper_nk":  pd.to_numeric(df["shipperID"], errors="coerce"),
            "company_name":df.get("companyName", pd.Series(dtype=str)),
            "phone":       df.get("phone", pd.Series(dtype=str)),
            "audit_sk":    -1,
        })
        sks = self._sk_gen.batch_next_sk("dim_shipper", len(out))
        out.insert(0, "shipper_sk", sks)
        return out

    # ------------------------------------------------------------------
    # Load methods (write to PostgreSQL)
    # ------------------------------------------------------------------

    def load_dim_to_postgres(
        self,
        dim_name: str,
        df: pd.DataFrame,
        engine: Engine,
        pk_col: str = "",
    ) -> int:
        """Upsert *df* into ``warehouse.{dim_name}``.

        For dim_date and Type-1 dims: INSERT ... ON CONFLICT DO NOTHING.
        For SCD-2 dims: same (initial load only; incremental handled by SCDManager).

        Args:
            dim_name: Table name (without schema prefix).
            df:       DataFrame to load.
            engine:   SQLAlchemy engine.
            pk_col:   Primary key column for conflict resolution (auto-detected if blank).

        Returns:
            Number of rows inserted.
        """
        if df.empty:
            logger.info("[DELIVER] %s: empty DataFrame, nothing to load", dim_name)
            return 0

        # Auto-detect PK column
        pk = pk_col or f"{dim_name.replace('dim_', '')}_sk"

        col_list = ", ".join(df.columns)
        placeholders = ", ".join(f":{c}" for c in df.columns)
        sql = text(f"""
            INSERT INTO warehouse.{dim_name} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """)

        rows = df.where(pd.notna(df), None).to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(sql, rows)
            # Update BIGSERIAL sequence so auto-inserts don't collide (skip for plain INT PKs)
            if pk in df.columns:
                max_sk = int(df[pk].max())
                seq_name = f"{dim_name}_{pk}_seq"
                seq_exists = conn.execute(text(
                    "SELECT COUNT(*) FROM pg_sequences "
                    "WHERE schemaname='warehouse' AND sequencename=:seq"
                ), {"seq": seq_name}).scalar()
                if seq_exists:
                    conn.execute(text(
                        f"SELECT setval('warehouse.{seq_name}', {max_sk + 1}, false)"
                    ))

        logger.info("[DELIVER] %s: inserted %d rows", dim_name, len(df))
        return len(df)

    def load_all_dims(
        self,
        batch_id: str,
        conformed_dir: Path,
        engine: Engine,
        raw_dir: Optional[Path] = None,
    ) -> dict[str, int]:
        """Build and load all dimension tables in dependency order.

        Load order: dim_date → dim_geography → dim_customer →
                    dim_product → dim_employee → dim_shipper.

        Args:
            batch_id:      ETL batch identifier.
            conformed_dir: Root of DimensionManager published dims.
            engine:        SQLAlchemy engine.
            raw_dir:       Raw CSV directory (fallback for unconfirmed entities).

        Returns:
            Dict of {dim_name: rows_loaded}.
        """
        loaded: dict[str, int] = {}
        raw = raw_dir or _find_latest_raw()

        # Ensure metadata.etl_runs record exists (required by dim_audit FK)
        _ensure_etl_run(batch_id, engine)

        # 1. dim_date
        logger.info("[DELIVER] batch=%s building dim_date", batch_id)
        date_df = self.build_dim_date()
        loaded["dim_date"] = self.load_dim_to_postgres("dim_date", date_df, engine, pk_col="date_sk")

        # 2. dim_geography
        geo_df = _load_conformed(conformed_dir, "dim_geography")
        if geo_df is not None and not geo_df.empty:
            geo_cols = ["geography_sk", "country_code", "country_name", "region", "subregion", "primary_currency"]
            geo_load = geo_df[[c for c in geo_cols if c in geo_df.columns]].copy()
            loaded["dim_geography"] = self.load_dim_to_postgres("dim_geography", geo_load, engine, pk_col="geography_sk")
        else:
            geo_load = pd.DataFrame()
            logger.warning("[DELIVER] dim_geography not found in conformed_dir")

        # 3. dim_customer
        cust_conf = _load_conformed(conformed_dir, "dim_customer")
        if cust_conf is not None and not cust_conf.empty:
            cust_df = self.build_dim_customer(cust_conf, geo_load)
            loaded["dim_customer"] = self.load_dim_to_postgres("dim_customer", cust_df, engine, pk_col="customer_sk")
        else:
            cust_df = pd.DataFrame()
            logger.warning("[DELIVER] dim_customer not found in conformed_dir")

        # 4. dim_product (from raw CSVs)
        products_df = _read_csv(raw, "products.csv")
        categories_df = _read_csv(raw, "categories.csv")
        suppliers_df = _read_csv(raw, "suppliers.csv")
        if not products_df.empty:
            prod_df = self.build_dim_product(products_df, categories_df, suppliers_df)
            loaded["dim_product"] = self.load_dim_to_postgres("dim_product", prod_df, engine, pk_col="product_sk")
        else:
            prod_df = pd.DataFrame()

        # 5. dim_employee
        employees_df = _read_csv(raw, "employees.csv")
        if not employees_df.empty:
            emp_df = self.build_dim_employee(employees_df)
            loaded["dim_employee"] = self.load_dim_to_postgres("dim_employee", emp_df, engine, pk_col="employee_sk")
        else:
            emp_df = pd.DataFrame()

        # 6. dim_shipper
        shippers_df = _read_csv(raw, "shippers.csv")
        if not shippers_df.empty:
            ship_df = self.build_dim_shipper(shippers_df)
            loaded["dim_shipper"] = self.load_dim_to_postgres("dim_shipper", ship_df, engine, pk_col="shipper_sk")
        else:
            ship_df = pd.DataFrame()

        return loaded


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _load_conformed(conformed_dir: Path, dim_name: str) -> Optional[pd.DataFrame]:
    path = conformed_dir / dim_name / "latest.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def _read_csv(raw_dir: Optional[Path], filename: str) -> pd.DataFrame:
    if raw_dir is None or not raw_dir.exists():
        return pd.DataFrame()
    path = raw_dir / filename
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, on_bad_lines="skip")
    except Exception as exc:
        logger.error("Failed to read %s: %s", path, exc)
        return pd.DataFrame()


def _find_latest_raw() -> Optional[Path]:
    raw_root = Path("data/raw/northwind")
    if not raw_root.exists():
        return None
    snapshots = sorted(raw_root.iterdir())
    return snapshots[-1] if snapshots else None


def _ensure_etl_run(batch_id: str, engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.etl_runs (batch_id, status)
            VALUES (:batch_id, 'RUNNING')
            ON CONFLICT (batch_id) DO NOTHING
        """), {"batch_id": batch_id})
